# ============================================
# Makefile para Compilação e Execução do Job Hadoop
# Disciplina: Sistemas Distribuídos
# Job: Análise de Logs com MapReduce
# ============================================

# Nome do arquivo fonte (código único)
SRC         = AnaliseDeTempo.java

# Nome do JAR de saída
JAR_FILE    = hadoop-log-analysis.jar

# Comandos e configurações
JAVAC       = javac
JAR         = jar
HADOOP      = hadoop
HADOOP_CP   = $(shell $(HADOOP) classpath 2>/dev/null)
REDUCERS    = 10  # Ajuste conforme o tamanho do cluster

# Alvos padrão
.PHONY: all compile jar clean run run-local help

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

# Executa o job no cluster Hadoop (usa dados do HDFS)
run: jar
	@echo "========================================"
	@echo "Executando job no cluster Hadoop..."
	@echo "Entrada: /input (diretório no HDFS)"
	@echo "Saída:  /output (diretório no HDFS)"
	@echo "Reducers: $(REDUCERS)"
	@echo "========================================"
	-$(HADOOP) fs -rm -r -f /output >/dev/null 2>&1
	$(HADOOP) jar $(JAR_FILE) AnaliseDeTempo -D mapreduce.job.reduces=$(REDUCERS) /input /output
	@echo "Job concluído! Salvando resultados..."
	$(HADOOP) fs -cat /output/part-00000 > resultados.csv
	@echo "Resultados salvos em: resultados.csv"
	@echo "Top 5 máquinas (tempo médio):"
	@head -5 resultados.csv

# Executa teste rápido com amostra (se existir sample.csv no HDFS)
run-local: jar
	@echo "Executando teste local com amostra..."
	-$(HADOOP) fs -rm -r -f /output >/dev/null 2>&1
	$(HADOOP) jar $(JAR_FILE) AnaliseDeTempo -D mapreduce.job.reduces=2 /input/amostra.txt /output
	@echo "Resultado do teste:"
	$(HADOOP) fs -cat /output/part-00000

# Verifica se os dados estão no HDFS
check-data:
	@echo "Verificando dados no HDFS:"
	$(HADOOP) fs -ls /input/
	@echo "Tamanho do dataset:"
	$(HADOOP) fs -du -h /input/cleaned_event_trace-v3.csv

# Limpa arquivos compilados localmente
clean:
	@echo "Limpando arquivos locais..."
	rm -f *.class $(JAR_FILE) resultados.csv

# Limpa HDFS (saída do job)
clean-hdfs:
	@echo "Limpando diretório /output no HDFS..."
	-$(HADOOP) fs -rm -r -f /output >/dev/null 2>&1
	@echo "Limpeza concluída!"

# Limpa tudo
clean-all: clean clean-hdfs

# Mostra ajuda
help:
	@echo "===================================================================="
	@echo "Makefile para Hadoop Job - Análise de Logs"
	@echo "===================================================================="
	@echo ""
	@echo "Comandos disponíveis:"
	@echo "  make compile      - Compila apenas o código Java"
	@echo "  make jar          - Compila e cria o JAR executável"
	@echo "  make run          - Executa o job no cluster Hadoop"
	@echo "  make run-local    - Executa teste com amostra pequena"
	@echo "  make check-data   - Verifica se os dados estão no HDFS"
	@echo "  make clean        - Remove arquivos compilados locais"
	@echo "  make clean-hdfs   - Remove saída do HDFS"
	@echo "  make clean-all    - Limpa tudo (local + HDFS)"
	@echo "  make help         - Mostra esta ajuda"
	@echo ""
	@echo "Configurações:"
	@echo "  REDUCERS=$(REDUCERS)  - Número de reducers (pode mudar: make run REDUCERS=20)"
	@echo ""
	@echo "Pré-requisitos:"
	@echo "  1. Dataset deve estar em: /input/cleaned_event_trace-v3.csv"
	@echo "  2. Hadoop deve estar configurado e rodando"
	@echo "  3. JAVA_HOME apontando para JDK 8"
	@echo "===================================================================="
