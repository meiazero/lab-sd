JAVAC       = javac
JAVA        = java
JAR         = jar
HADOOP      = hadoop
HADOOP_CP   = $(shell $(HADOOP) classpath 2>/dev/null)

# Arquivos do job Hadoop (API mapred)
JOB_SOURCES = Map.java Reduce.java main.java
JAR_FILE    = lab1.jar

.PHONY: all jar run clean very-clean

all: jar

# Compila as classes do job e empacota em um JAR simples
jar: $(JOB_SOURCES)
	$(JAVAC) -source 1.8 -target 1.8 -cp "$(HADOOP_CP)" $(JOB_SOURCES)
	$(JAR) cfe $(JAR_FILE) Main *.class

# Executa o job em um Hadoop instalado (usa o 'hadoop classpath')
# Alvo didático: entrada local 'event-trace.txt' e saída 'out'
run: jar
	-$(HADOOP) fs -rm -r -f out >/dev/null 2>&1 || true
	$(HADOOP) jar $(JAR_FILE) Main event-trace.txt out 1
	$(HADOOP) fs -cat out/part-*

# Execução em cluster (requer HDFS/YARN). Forneça HDFS_IN e HDFS_OUT.
run-hdfs: jar
	@test -n "$(HDFS_IN)" || (echo "Defina HDFS_IN (entrada no HDFS)" && exit 1)
	@test -n "$(HDFS_OUT)" || (echo "Defina HDFS_OUT (saída no HDFS)" && exit 1)
	-$(HADOOP) fs -rm -r -f $(HDFS_OUT) >/dev/null 2>&1 || true
	$(HADOOP) jar $(JAR_FILE) Main $(HDFS_IN) $(HDFS_OUT) $(or $(REDUCERS),1)
	$(HADOOP) fs -cat $(HDFS_OUT)/part-*

clean:
	rm -f *.class $(JAR_FILE)

very-clean: clean
	-$(HADOOP) fs -rm -r -f out >/dev/null 2>&1 || true
