import h3
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType

import common as C

MAX_QUARANTINE_RATE = 0.05
CATEGORY_COLS = ["landslide_category", "landslide_trigger", "landslide_size", "landslide_setting"]


def read_raw(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [StructField(c, StringType()) for c in C.RAW_COLUMNS]
        + [StructField("_corrupt_record", StringType())]
    )
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("enforceSchema", False)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(C.LANDING_CSV)
    )


def normalize(raw: DataFrame) -> DataFrame:
    s = lambda name: C.clean_str(F.col(name))  # noqa: E731
    return raw.select(
        s("event_id").try_cast("bigint").alias("event_id"),
        C.parse_ts(s("event_date"), C.DATE_PATTERNS).alias("event_ts"),
        s("event_date").alias("raw_event_date"),
        C.parse_ts(s("last_edited_date"), C.DATE_PATTERNS).alias("last_edited_ts"),
        F.upper(s("country_code")).alias("country_code"),
        s("country_name").alias("country_name"),
        s("admin_division_name").alias("admin_division"),
        *[C.snake(F.col(c)).alias(c) for c in CATEGORY_COLS],
        C.accuracy_km(F.col("location_accuracy")).alias("location_accuracy_km"),
        s("fatality_count").try_cast("int").alias("fatality_count"),
        s("injury_count").try_cast("int").alias("injury_count"),
        s("latitude").try_cast("double").alias("latitude"),
        s("longitude").try_cast("double").alias("longitude"),
        s("event_title").alias("event_title"),
        s("event_description").alias("event_description"),
        s("location_description").alias("location_description"),
        s("source_name").alias("source_name"),
    )


def flag_rejects(df: DataFrame) -> DataFrame:
    false = F.lit(False)
    latest_first = Window.partitionBy("event_id").orderBy(
        F.col("last_edited_ts").desc_nulls_last(), F.col("event_title").asc_nulls_last()
    )
    rules = {
        "null_event_id": F.col("event_id").isNull(),
        "null_event_ts": F.col("event_ts").isNull(),
        "bad_latitude": ~F.coalesce(F.col("latitude").between(-90, 90), false),
        "bad_longitude": ~F.coalesce(F.col("longitude").between(-180, 180), false),
        "null_island": F.coalesce((F.col("latitude") == 0) & (F.col("longitude") == 0), false),
        "negative_fatalities": F.coalesce(F.col("fatality_count") < 0, false),
        "negative_injuries": F.coalesce(F.col("injury_count") < 0, false),
        "after_export_date": F.coalesce(
            F.to_date("event_ts") > F.lit(C.EXPORT_DATE).cast("date"), false
        ),
        "duplicate_event_id": F.col("event_id").isNotNull()
        & (F.row_number().over(latest_first) > 1),
    }
    for col, allowed in C.ENUMS.items():
        rules[f"bad_{col}"] = F.col(col).isNotNull() & ~F.col(col).isin(*allowed)

    reasons = F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    return df.withColumn("reject_reason", F.nullif(reasons, F.lit("")))


def h3_udf(res: int):
    @pandas_udf("long")  # type: ignore
    def _h3(lat: pd.Series, lng: pd.Series) -> pd.Series:
        return pd.Series([h3.str_to_int(h3.latlng_to_cell(a, b, res)) for a, b in zip(lat, lng)])

    return _h3


def enrich(clean: DataFrame, run_id: str) -> DataFrame:
    return (
        clean.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_year", F.year("event_ts").cast("smallint"))
        .withColumn("event_month", F.month("event_ts").cast("tinyint"))
        .withColumn("h3_r5", h3_udf(5)("latitude", "longitude"))  # type: ignore
        .withColumn("h3_r7", h3_udf(7)("latitude", "longitude"))  # type: ignore
        .withColumn("is_synthetic", F.lit(False))
        .withColumn("ingest_run_id", F.lit(run_id))
        .select(*C.CANONICAL_COLUMNS)
    )


def main() -> None:
    run_id = C.new_run_id()
    log = C.RunLog(run_id)
    spark = C.get_spark("ingest_normalize")

    with log.step("read_raw"):
        raw = read_raw(spark).cache()
        n_in = raw.count()
        n_corrupt = raw.filter(F.col("_corrupt_record").isNotNull()).count()
    assert n_corrupt == 0, f"{n_corrupt} corrupt CSV rows"
    if C.RAW_ROW_COUNT is not None:
        assert n_in == C.RAW_ROW_COUNT, f"read {n_in} rows, profile said {C.RAW_ROW_COUNT}"

    with log.step("normalize_validate"):
        flagged = flag_rejects(normalize(raw.drop("_corrupt_record"))).cache()
        bad = flagged.filter("reject_reason IS NOT NULL")
        clean = flagged.filter("reject_reason IS NULL").drop(
            "reject_reason", "raw_event_date", "last_edited_ts"
        )
        n_clean, n_bad = clean.count(), bad.count()
        assert n_clean + n_bad == n_in, f"{n_clean} + {n_bad} != {n_in}"
        bad.write.mode("overwrite").parquet(f"{C.QUARANTINE_ROOT}/run_id={run_id}")

    rate = n_bad / n_in
    print(f"rows in={n_in} clean={n_clean} quarantined={n_bad} ({rate:.2%})")
    flagged.groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)
    log.data["counts"].update(raw=n_in, clean=n_clean, quarantined=n_bad, quarantine_rate=rate)
    if rate > MAX_QUARANTINE_RATE:
        log.save()
        raise SystemExit(f"quarantine rate {rate:.2%} > {MAX_QUARANTINE_RATE:.0%}: check rules")

    with log.step("enrich_write_sf1"):
        sf1 = enrich(clean, run_id)
        C.assert_schema(sf1)
        (
            sf1.coalesce(1).sortWithinPartitions(*C.SORT_KEYS)
            .write.mode("overwrite").parquet(C.canonical_path(1))
        )

    back = spark.read.parquet(C.canonical_path(1))
    assert back.count() == n_clean
    C.assert_schema(back)
    assert back.filter("h3_r5 IS NULL OR h3_r7 IS NULL").count() == 0
    log.data["counts"]["sf1_rows"] = n_clean
    log.save()
    print(f"OK: {C.canonical_path(1)} ({n_clean} rows), run record {log.path}")
    spark.stop()


if __name__ == "__main__":
    main()
