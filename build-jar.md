# Build JAR

## Requisitos

- Apache Hadoop v2.9.2
> Faça o download do haddop usando esse link: https://drive.google.com/file/d/1IPe4ObZrVhstvGdTXil5jfE8LxKw437w/view?usp=sharing
> ou clicando [aqui](https://drive.google.com/file/d/1IPe4ObZrVhstvGdTXil5jfE8LxKw437w/view?usp=sharing)
- [Docker & Docker Compose](https://www.docker.com/get-started/)

## Configurando o ambiente para compilar o JAR

1. Crie um diretório para o projeto
2. Mova o arquivo `hadoop-2.9.2.tar.gz` para o diretório criado
3. Dentro do diretório, crie um arquivo nomeado `Dockerfile` com o seguinte conteúdo:

```dockerfile
FROM eclipse-temurin:8-jdk AS base

ARG HADOOP_VERSION=2.9.2

RUN apt-get update && \
  apt-get install -y --no-install-recommends curl ca-certificates make && \
  rm -rf /var/lib/apt/lists/*

COPY hadoop-2.9.2.tar.gz /tmp/hadoop.tar.gz

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

```

4. Crie um arquivo `docker-compose.yaml` com o seguinte conteúdo:

```yaml
services:
  build-jar:
    build:
      context: .
      dockerfile: Dockerfile
    # Copia o artefato do container para o host via volume
    command: ["/bin/sh", "-c", "cp /artifact/lab1.jar /out/lab1.jar"]
    volumes:
      - ./dist:/out
```

5. Crie um arquivo `Makefile` com o seguinte conteúdo:

```makefile
# Nome do arquivo fonte (código único)
SRC         = <NomeDoArquivo>.java

# Nome do JAR de saída
JAR_FILE    = hadoop-log-analysis.jar

# Comandos e configurações
JAVAC       = javac
JAR         = jar
HADOOP      = hadoop
HADOOP_CP   = $(shell $(HADOOP) classpath 2>/dev/null)

# Alvos padrão
.PHONY: all compile jar

# Alvo principal: compilar e criar JAR
all: jar

# Compila o código Java com classpath do Hadoop
compile:
 @echo "Compilando $(SRC)..."
 $(JAVAC) -source 1.8 -target 1.8 -cp "$(HADOOP_CP)" $(SRC)
 @echo "Compilação concluída!"

# Cria o JAR executável (depende da compilação)
jar: compile
 @echo "Criando JAR: $(JAR_FILE)"
 $(JAR) cf $(JAR_FILE) *.class
 @echo "JAR criado com sucesso!"

```
> Observação: o nome do arquivo fonte deve ser substituído pelo nome do arquivo fonte que você criou.

## Executando o docker-compose

1. Verifique se o docker-compose está no mesmo diretório que o Dockerfile
2. Execute o comando `docker-compose up`
3. O JAR será copiado para o diretório `dist` no host
