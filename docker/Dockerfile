# One image for every node of the cluster (master, workers, client), so that
# Hadoop, Spark, Java, Python and NumPy are *identical* everywhere.
FROM eclipse-temurin:8-jre-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.3
ARG APACHE_MIRROR=https://archive.apache.org/dist

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      python3 python3-numpy python3-scipy python3-matplotlib \
      curl ca-certificates gettext-base procps tini zip unzip netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fSL --retry 5 "${APACHE_MIRROR}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
      | tar -xz -C /opt \
 && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm -rf /opt/hadoop/share/doc

RUN curl -fSL --retry 5 "${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      | tar -xz -C /opt \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8 \
    PROJECT_DIR=/opt/project \
    LOCAL_DATA=/localdata \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    MAPRED_HISTORYSERVER_USER=root
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:${PATH}

WORKDIR /opt/project
ENTRYPOINT ["/usr/bin/tini", "--", "bash", "/opt/project/docker/entrypoint.sh"]
CMD ["client"]
