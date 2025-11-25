import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * Classe principal para análise de tempo de atividade de máquinas em um cluster Hadoop.
 * Este job MapReduce filtra eventos ativos (event_type == 1) e calcula estatísticas
 * para máquinas que foram ativas por pelo menos 300 dias com tempo médio >= 1 hora por dia.
 * 
 * Métricas calculadas:
 * - Dias ativos: período total observado (span) em dias.
 * - Tempo médio por dia: duração total ativa dividida pelos dias ativos.
 * 
 * Nota estatística: O tempo médio por dia é calculado sobre o span total, incluindo
 * possíveis dias inativos. Isso representa a média diária ao longo do período observado,
 * não necessariamente a média apenas nos dias com atividade.
 */
public class AnaliseDeTempo {

    // Constantes para cálculos de tempo
    private static final long SECONDS_PER_DAY = 86400L; // Segundos em um dia
    private static final double MIN_ACTIVE_DAYS = 300.0; // Mínimo de dias ativos
    private static final double MIN_AVG_SECONDS_PER_DAY = 3600.0; // Mínimo 1 hora por dia em segundos

    /**
     * Classe Mapper: Processa cada linha do arquivo de entrada.
     * Filtra eventos ativos (event_type == 1) e emite (machineId, start:end).
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable machineId = new LongWritable();
        private Text timeRange = new Text();

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            // Ignora comentários e linhas vazias
            if (line.startsWith("#") || line.trim().isEmpty()) return;

            String[] tokens = line.split("\\s+");

            // Formato esperado: index, machineId, eventType, start, end
            if (tokens.length >= 5) {
                String eventType = tokens[2];
                // Filtra apenas eventos ativos (event_type == 1)
                if ("1".equals(eventType)) {
                    try {
                        long id = Long.parseLong(tokens[1]);
                        String start = tokens[3];
                        String end = tokens[4];
                        
                        machineId.set(id);
                        timeRange.set(start + ":" + end);
                        output.collect(machineId, timeRange);
                    } catch (NumberFormatException e) {
                        // Ignora linhas malformadas
                    }
                }
            }
        }
    }

    /**
     * Classe Reducer: Agrega dados por machineId e calcula estatísticas.
     * Verifica critérios de atividade e emite resultados em formato CSV.
     */
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        private Text resultValue = new Text();

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            long totalDurationSeconds = 0; // Duração total ativa em segundos
            long traceStart = Long.MAX_VALUE; // Timestamp inicial do período observado
            long traceEnd = Long.MIN_VALUE;   // Timestamp final do período observado
            boolean hasData = false;

            // Processa todos os intervalos de tempo para esta máquina
            while (values.hasNext()) {
                String line = values.next().toString();
                String[] tokens = line.split(":");
                
                if (tokens.length < 2) continue;

                try {
                    // Converte timestamps para long (segundos desde epoch)
                    long start = (long) Double.parseDouble(tokens[0]);
                    long end = (long) Double.parseDouble(tokens[1]);

                    // Atualiza limites do período observado
                    if (start < traceStart) traceStart = start;
                    if (end > traceEnd) traceEnd = end;

                    // Soma duração ativa (assume não há sobreposições nos dados)
                    if (end > start) {
                        totalDurationSeconds += (end - start);
                    }
                    hasData = true;
                } catch (NumberFormatException e) {
                    continue; // Ignora erros de parsing
                }
            }

            if (!hasData) return;

            // Calcula span total em segundos
            long spanSeconds = traceEnd - traceStart;
            if (spanSeconds <= 0) return;

            // Converte span para dias
            double daysActive = spanSeconds / (double) SECONDS_PER_DAY;

            // Critério 1: Máquina deve ter sido observada por pelo menos 300 dias
            if (daysActive >= MIN_ACTIVE_DAYS) {
                
                // Critério 2: Tempo médio ativo >= 1 hora por dia
                double averageSecondsPerDay = totalDurationSeconds / daysActive;

                if (averageSecondsPerDay >= MIN_AVG_SECONDS_PER_DAY) {
                    
                    // Converte para horas para legibilidade
                    double averageHoursPerDay = averageSecondsPerDay / 3600.0;

                    // Formato CSV: avg_hours_day, span_days, start_epoch, end_epoch
                    // Exemplo: 1.50, 305.20, 1211524407, 1212269739
                    String resultString = String.format("%.4f,%.2f,%d,%d", 
                                                        averageHoursPerDay, daysActive, traceStart, traceEnd);
                    
                    resultValue.set(resultString);
                    output.collect(key, resultValue);
                }
            }
        }
    }

    /**
     * Método main: Configura e executa o job MapReduce.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: analisetempo <in> <out>");
            System.exit(1);
        }

        JobConf conf = new JobConf(AnaliseDeTempo.class);
        conf.setJobName("analisetempo_tarefa2");
        
        // Define separador do arquivo de saída como vírgula para formato CSV
        conf.set("mapred.textoutputformat.separator", ",");

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}