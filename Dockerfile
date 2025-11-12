FROM eclipse-temurin:8-jdk AS base

ARG HADOOP_VERSION=2.9.2

RUN apt-get update && \
  apt-get install -y --no-install-recommends curl ca-certificates make && \
  rm -rf /var/lib/apt/lists/*

COPY hadoop-2.9.2.tar.gz /tmp/hadoop.tar.gz

# https://archive.apache.org/dist/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz
RUN tar -xzf /tmp/hadoop.tar.gz -C /opt && \
  ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
  rm /tmp/hadoop.tar.gz

ENV HADOOP_HOME=/opt/hadoop \
  HADOOP_COMMON_HOME=/opt/hadoop \
  HADOOP_HDFS_HOME=/opt/hadoop \
  HADOOP_MAPRED_HOME=/opt/hadoop \
  HADOOP_YARN_HOME=/opt/hadoop \
  PATH=/opt/hadoop/bin:$PATH

WORKDIR /app
COPY . /app

FROM base AS builder

# Compila e empacota o jar (usa o Makefile que referencia o classpath do Hadoop)
RUN make clean jar

# Stage 2: imagem final mínima contendo apenas o artefato
FROM alpine:3.20 AS artifact
WORKDIR /artifact

# Copia o JAR gerado pelo Makefile e o publica com o nome esperado pelo docker-compose
# Makefile gera hadoop-log-analysis.jar em /app; renomeamos para lab1.jar
COPY --from=builder /app/hadoop-log-analysis.jar /artifact/lab1.jar

# Comando padrão opcional (será sobrescrito pelo docker-compose); útil para depuração manual
CMD ["/bin/sh", "-c", "echo 'lab1.jar pronto em /artifact/lab1.jar'; ls -l /artifact"]
