FROM eclipse-temurin:8-jdk AS builder

ARG HADOOP_VERSION=3.3.6

RUN apt-get update && \
  apt-get install -y --no-install-recommends curl ca-certificates make && \
  rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -o /tmp/hadoop.tar.gz && \
  tar -xzf /tmp/hadoop.tar.gz -C /opt && \
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

# Compila e empacota o jar (usa o Makefile que referencia o classpath do Hadoop)
RUN make jar

# Stage 2: imagem final mínima contendo apenas o artefato
FROM alpine:3.20 AS artifact
WORKDIR /
COPY --from=builder /app/lab1.jar /artifact/lab1.jar

# Sem comando padrão: esta imagem é apenas um portador do artefato
CMD ["/bin/sh", "-c", "echo 'lab1.jar pronto em /artifact/lab1.jar'"]
