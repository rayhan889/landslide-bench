ARG HADOOP_JRE_IMAGE=eclipse-temurin:11-jre-jammy
FROM ${HADOOP_JRE_IMAGE}

ARG HADOOP_VERSION
ARG HADOOP_SHA512

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl procps \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/hadoop.tgz \
      "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-lean.tar.gz" \
 && echo "${HADOOP_SHA512}  /tmp/hadoop.tgz" | sha512sum -c - \
 && tar -xzf /tmp/hadoop.tgz -C /opt \
 && ln -s "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
 && rm /tmp/hadoop.tgz

RUN useradd --uid 1000 --create-home hadoop \
 && mkdir -p /data/hdfs/name /data/hdfs/data \
 && chown -R hadoop:hadoop /data /opt/hadoop-${HADOOP_VERSION}

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:$PATH

COPY entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

USER hadoop
ENTRYPOINT ["/entrypoint.sh"]
