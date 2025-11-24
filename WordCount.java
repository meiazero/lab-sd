import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// Usando a API antiga (mapred) conforme os slides
import org.apache.hadoop.mapred.*; 

public class TempoCount {

    // Mapper conforme Slide 58
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable k = new LongWritable();
        private Text v = new Text();

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            // Evita linhas de comentário ou cabeçalho
            if (line.startsWith("#")) return;

            String[] tokens = line.split("\\s+"); // Split por espaço em branco

            // Verificação básica de tamanho para evitar ArrayIndexOutOfBounds
            if (tokens.length >= 5) {
                // tokens[1] é o ID da máquina, tokens[2] é o event_type
                // tokens[3] start_time, tokens[4] end_time
                if (tokens[2].equals("1")) { // Se event_type for 1 (ligada)
                    long machineId = Long.parseLong(tokens[1]);
                    k.set(machineId);
                    v.set(tokens[3] + ":" + tokens[4]); // Envia "inicio:fim"
                    output.collect(k, v);
                }
            }
        }
    }

    // Reducer conforme Slides 60-61
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        private Text val = new Text();

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            long sum = 0;
            // Variáveis para rastrear o intervalo total (não usadas no output do Ex 1, mas presentes no código do slide)
            long traceStart = Long.MAX_VALUE;
            long traceEnd = 0;

            while (values.hasNext()) {
                String line = values.next().toString();
                String[] tokens = line.split(":");
                
                // Convertendo double para long (timestamps podem ter ponto flutuante no arquivo)
                long start = (long) Double.parseDouble(tokens[0]);
                long end = (long) Double.parseDouble(tokens[1]);

                if (start < traceStart) traceStart = start;
                if (end > traceEnd) traceEnd = end;

                sum += (end - start); // Soma a duração
            }

            val.set(Long.toString(sum)); // Define a soma total como saída
            output.collect(key, val);
        }
    }

    // Main conforme Slide 62
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: tempocount <in> <out> [num_reduces]");
            System.exit(1);
        }

        JobConf conf = new JobConf(TempoCount.class);
        conf.setJobName("tempocount");

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        if (args.length > 2) {
             conf.setNumReduceTasks(Integer.parseInt(args[2]));
        }

        JobClient.runJob(conf);
    }
}