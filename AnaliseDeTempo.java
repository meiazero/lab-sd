import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class AnaliseDeTempo {

    private static final long SECONDS_PER_DAY = 86400L;
    private static final double MIN_ACTIVE_DAYS = 300.0;
    private static final double MIN_AVG_SECONDS_PER_DAY = 3600.0;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable machineId = new LongWritable();
        private Text timeRange = new Text();

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            if (line.startsWith("#") || line.trim().isEmpty()) return;

            String[] tokens = line.split("\\s+");

            // Expected format: index, machineId, eventType, start, end
            if (tokens.length >= 5) {
                String eventType = tokens[2];
                // Filter for active events (event_type == 1)
                if ("1".equals(eventType)) {
                    try {
                        long id = Long.parseLong(tokens[1]);
                        String start = tokens[3];
                        String end = tokens[4];
                        
                        machineId.set(id);
                        timeRange.set(start + ":" + end);
                        output.collect(machineId, timeRange);
                    } catch (NumberFormatException e) {
                        // Ignore malformed lines
                    }
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        private Text resultValue = new Text();

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            long totalDurationSeconds = 0;
            long traceStart = Long.MAX_VALUE;
            long traceEnd = Long.MIN_VALUE;
            boolean hasData = false;

            while (values.hasNext()) {
                String line = values.next().toString();
                String[] tokens = line.split(":");
                
                if (tokens.length < 2) continue;

                try {
                    // Parse timestamps (seconds)
                    long start = (long) Double.parseDouble(tokens[0]);
                    long end = (long) Double.parseDouble(tokens[1]);

                    if (start < traceStart) traceStart = start;
                    if (end > traceEnd) traceEnd = end;

                    if (end > start) {
                        totalDurationSeconds += (end - start);
                    }
                    hasData = true;
                } catch (NumberFormatException e) {
                    continue;
                }
            }

            if (!hasData) return;

            long spanSeconds = traceEnd - traceStart;
            if (spanSeconds <= 0) return;

            double daysActive = spanSeconds / (double) SECONDS_PER_DAY;

            // Requisito 1: Máquina ativa por pelo menos 300 dias
            if (daysActive >= MIN_ACTIVE_DAYS) {
                
                // Requisito 2: Tempo médio >= 1 hora por dia
                double averageSecondsPerDay = totalDurationSeconds / daysActive;

                if (averageSecondsPerDay >= MIN_AVG_SECONDS_PER_DAY) {
                    
                    // Normalização para horas (mais legível para humanos)
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

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: analisetempo <in> <out>");
            System.exit(1);
        }

        JobConf conf = new JobConf(AnaliseDeTempo.class);
        conf.setJobName("analisetempo_tarefa2");
        
        // Define o separador do arquivo de saída como vírgula para formato CSV
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