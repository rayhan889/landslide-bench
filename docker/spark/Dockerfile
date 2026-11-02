ARG SPARK_BASE_IMAGE
ARG UV_VERSION

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM ${SPARK_BASE_IMAGE}
ARG ICEBERG_VERSION
ARG ICEBERG_SHA1

USER root
COPY --from=uv /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && JAR="iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
 && curl -fsSL -o "/opt/spark/jars/${JAR}" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/${JAR}" \
 && echo "${ICEBERG_SHA1}  /opt/spark/jars/${JAR}" | sha1sum -c -

ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /build
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project \
 && chmod -R a+rX /opt/python /opt/venv

ENV PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

WORKDIR /workspace
USER spark
