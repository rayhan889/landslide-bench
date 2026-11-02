import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType,
    ShortType, StringType, StructField, StructType, TimestampType,
)

SCHEMA_VERSION = "schema_v1"
EXPORT_DATE = "2016-03-07"

HDFS_ROOT = "hdfs://namenode:8020/landslide"
LANDING_CSV = f"{HDFS_ROOT}/landing/glc/export={EXPORT_DATE}/glc.csv"
QUARANTINE_ROOT = f"{HDFS_ROOT}/quarantine"
RUNS_DIR = Path("/workspace/results/runs")


def canonical_path(sf: int) -> str:
    return f"{HDFS_ROOT}/canonical/sf={sf}"

RAW_ROW_COUNT = None
RAW_COLUMNS = [
    "source_name", "source_link", "event_id", "event_date", "event_time", "event_title",
    "event_description", "location_description", "location_accuracy", "landslide_category",
    "landslide_trigger", "landslide_size", "landslide_setting", "fatality_count",
    "injury_count", "storm_name", "photo_link", "notes", "event_import_source",
    "event_import_id", "country_name", "country_code", "admin_division_name",
    "admin_division_population", "gazeteer_closest_point", "gazeteer_distance",
    "submitted_date", "created_date", "last_edited_date", "longitude", "latitude",
]

DATE_PATTERNS = ["M/d/yyyy h:mm:ss a", "M/d/yyyy H:mm", "yyyy-MM-dd'T'HH:mm:ss"]

ENUMS: dict[str, list[str]] = {
    # "landslide_category": ["landslide", "mudslide", ...],
}

CANONICAL_SCHEMA = StructType([
    StructField("event_id", LongType()),
    StructField("event_ts", TimestampType()),
    StructField("event_date", DateType()),
    StructField("event_year", ShortType()),
    StructField("event_month", ByteType()),
    StructField("country_code", StringType()),
    StructField("country_name", StringType()),
    StructField("admin_division", StringType()),
    StructField("landslide_category", StringType()),
    StructField("landslide_trigger", StringType()),
    StructField("landslide_size", StringType()),
    StructField("landslide_setting", StringType()),
    StructField("location_accuracy_km", FloatType()),
    StructField("fatality_count", IntegerType()),
    StructField("injury_count", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("h3_r5", LongType()),
    StructField("h3_r7", LongType()),
    StructField("event_title", StringType()),
    StructField("event_description", StringType()),
    StructField("location_description", StringType()),
    StructField("source_name", StringType()),
    StructField("is_synthetic", BooleanType()),
    StructField("ingest_run_id", StringType()),
])
CANONICAL_COLUMNS = [f.name for f in CANONICAL_SCHEMA.fields]
SORT_KEYS = ["country_code", "h3_r5", "event_ts", "event_id"]


def assert_schema(df: DataFrame) -> None:
    got = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    want = [(f.name, f.dataType.simpleString()) for f in CANONICAL_SCHEMA.fields]
    assert got == want, f"not {SCHEMA_VERSION}:\n got  {got}\n want {want}"


def clean_str(c: Column) -> Column:
    return F.nullif(F.trim(c), F.lit(""))


def snake(c: Column) -> Column:
    s = F.regexp_replace(F.lower(F.trim(c)), r"[^a-z0-9]+", "_")
    return F.nullif(F.regexp_replace(s, r"^_+|_+$", ""), F.lit(""))


def parse_ts(c: Column, patterns: list[str]) -> Column:
    return F.coalesce(*[F.try_to_timestamp(c, F.lit(p)) for p in patterns])


def accuracy_km(c: Column) -> Column:
    s = F.lower(F.trim(c))
    km = F.regexp_extract(s, r"^(\d+(?:\.\d+)?)\s*km$", 1)
    return (
        F.when(s == "exact", F.lit(0.0))
        .when(km != "", km.try_cast("double"))
        .cast("float")
    )


def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def new_run_id() -> str:
    if os.environ.get("RUN_ID"):
        return os.environ["RUN_ID"]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{os.environ.get('GIT_COMMIT', 'nogit')}"


class RunLog:
    def __init__(self, run_id: str):
        self.path = RUNS_DIR / run_id / "run.json"
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {
            "run_id": run_id, "git_commit": os.environ.get("GIT_COMMIT", "nogit"),
            "schema_version": SCHEMA_VERSION, "steps_sec": {}, "counts": {},
        }

    @contextmanager
    def step(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.data["steps_sec"][name] = round(time.perf_counter() - t0, 3)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, default=str))
