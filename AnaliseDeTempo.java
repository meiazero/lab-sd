import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class AnaliseDeTempo {

    // O Mapper permanece idêntico ao do Exercício 1
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable k = new LongWritable();
        private Text v = new Text();

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            if (line.startsWith("#")) return;

            String[] tokens = line.split("\\s+");

            if (tokens.length >= 5) {
                // Filtra apenas eventos onde a máquina estava ligada (event_type == 1)
                if (tokens[2].equals("1")) {
                    long machineId = Long.parseLong(tokens[1]);
                    k.set(machineId);
                    v.set(tokens[3] + ":" + tokens[4]); 
                    output.collect(k, v);
                }
            }
        }
    }

    // Reducer modificado para a Tarefa 2
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        private Text val = new Text();

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            long totalDurationSeconds = 0;
            long traceStart = Long.MAX_VALUE;
            long traceEnd = 0;

            // Itera sobre todos os eventos da máquina
            while (values.hasNext()) {
                String line = values.next().toString();
                String[] tokens = line.split(":");
                
                long start = (long) Double.parseDouble(tokens[0]);
                long end = (long) Double.parseDouble(tokens[1]);

                // Encontra o primeiro start e o último end para saber o período de atividade
                if (start < traceStart) traceStart = start;
                if (end > traceEnd) traceEnd = end;

                // Soma o tempo que ficou ligada
                totalDurationSeconds += (end - start);
            }

            // Lógica da Tarefa 2
            // 1. Calcular quantos dias a máquina esteve ativa no sistema (Intervalo total)
            long spanSeconds = traceEnd - traceStart;
            // Evita divisão por zero
            if (spanSeconds <= 0) return;

            double daysActive = spanSeconds / 86400.0; // 86400 segundos em um dia

            // Requisito 1: Máquina ativa por 300 dias (considerando o span de tempo)
            if (daysActive >= 300.0) {
                
                // Requisito 2: Tempo médio maior ou igual a 1 hora por dia
                double averageSecondsPerDay = totalDurationSeconds / daysActive;

                if (averageSecondsPerDay >= 3600.0) { // 3600 segundos = 1 hora
                    
                    // Formata a saída: "TempoMédio(s)  DataInicio  DataFim"
                    // (Você pode formatar TempoMédio para horas se preferir, aqui está em segundos/dia)
                    String resultString = String.format("%.2f s/dia\tInicio:%d\tFim:%d", 
                                                        averageSecondsPerDay, traceStart, traceEnd);
                    
                    val.set(resultString);
                    output.collect(key, val);
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