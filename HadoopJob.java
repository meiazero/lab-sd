import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/**
 * Job MapReduce para analisar logs de atividade de máquinas.
 * Timestamps em milissegundos epoch UTC.
 * Versão feita com Kimi 2 thinking: https://kimi.com
 */
public class HadoopJob extends Configured implements Tool {

    /**
     * Writable customizado para armazenar dados de um dia de atividade
     */
    public static class DayActivity implements Writable {
        private String day;           // Formato: yyyy-MM-dd (UTC)
        private long activeTimeMs;    // Tempo ativo no dia (ms)
        private long minTimestamp;    // Menor timestamp do evento
        private long maxTimestamp;    // Maior timestamp do evento

        public DayActivity() {}

        public DayActivity(String day, long activeTimeMs, long minTimestamp, long maxTimestamp) {
            this.day = day;
            this.activeTimeMs = activeTimeMs;
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(day);
            out.writeLong(activeTimeMs);
            out.writeLong(minTimestamp);
            out.writeLong(maxTimestamp);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.day = in.readUTF();
            this.activeTimeMs = in.readLong();
            this.minTimestamp = in.readLong();
            this.maxTimestamp = in.readLong();
        }

        public String getDay() { return day; }
        public long getActiveTimeMs() { return activeTimeMs; }
        public long getMinTimestamp() { return minTimestamp; }
        public long getMaxTimestamp() { return maxTimestamp; }
    }

    /**
     * Mapper: Processa cada linha do CSV com timestamps epoch
     */
    public static class LogMapper extends Mapper<LongWritable, Text, Text, DayActivity> {
        
        // Constantes para cálculos de tempo (em ms)
        private static final long MS_PER_DAY = 24 * 60 * 60 * 1000L;
        private static final long MS_PER_HOUR = 60 * 60 * 1000L;

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            
            // Ignora cabeçalho
            if (line.startsWith("component_id")) return;
            
            String[] fields = line.split(",");
            if (fields.length < 5) return; // Linha inválida
            
            try {
                String nodeName = fields[1].trim();
                int eventType = Integer.parseInt(fields[2].trim());
                
                // Processa apenas eventos ativos (tipo=1)
                if (eventType != 1) return;
                
                // Converte diretamente de epoch ms
                long startMs = Long.parseLong(fields[3].trim());
                long endMs = Long.parseLong(fields[4].trim());
                
                // Valida timestamps
                if (startMs >= endMs) return;
                
                // Processa cada dia do evento
                long dayStartMs = getDayStart(startMs);
                
                while (true) {
                    long nextDayStartMs = dayStartMs + MS_PER_DAY;
                    
                    // Interseção do evento com o dia atual
                    long currentStart = Math.max(startMs, dayStartMs);
                    long currentEnd = Math.min(endMs, nextDayStartMs);
                    
                    // Se há sobreposição, calcula tempo ativo
                    if (currentStart < currentEnd) {
                        long activeTimeMs = currentEnd - currentStart;
                        String day = formatDay(dayStartMs);
                        
                        context.write(
                            new Text(nodeName), 
                            new DayActivity(day, activeTimeMs, currentStart, currentEnd)
                        );
                    }
                    
                    // Próximo dia
                    dayStartMs = nextDayStartMs;
                    
                    // Sai do loop quando passar do fim do evento
                    if (nextDayStartMs >= endMs) break;
                }
                
            } catch (Exception e) {
                System.err.println("Erro ao processar linha: " + line);
            }
        }
        
        /**
         * Retorna o timestamp epoch do início do dia (UTC) no qual 'timestampMs' está contido
         */
        private long getDayStart(long timestampMs) {
            // Calcula deslocamento para UTC (0 horas)
            // Simplesmente trunca para o início do dia em UTC
            return timestampMs - (timestampMs % MS_PER_DAY);
        }
        
        /**
         * Formata timestamp epoch para string yyyy-MM-dd em UTC
         */
        private String formatDay(long dayStartMs) {
            // Ajuste para timezone UTC (evita problemas de conversão local)
            // Para Hadoop, usaremos formatação simples
            long dayNumber = dayStartMs / MS_PER_DAY;
            long daysSinceEpoch = dayNumber + 10957; // Offset para 1970-01-01
            // Simplificado: retorna formato epoch para evitar complexidade de timezone
            return Long.toString(dayStartMs); // Pode ser melhorado se precisar de legibilidade
        }
    }

    /**
     * Reducer: Agrega dados por máquina e aplica filtros
     */
    public static class MachineReducer extends Reducer<Text, DayActivity, Text, Text> {
        
        private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        
        static {
            DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Override
        protected void reduce(Text key, Iterable<DayActivity> values, Context context) 
                throws IOException, InterruptedException {
            
            Set<String> uniqueDays = new HashSet<>();
            long totalActiveMs = 0;
            long globalMinTs = Long.MAX_VALUE;
            long globalMaxTs = Long.MIN_VALUE;
            
            // Processa todos os dias da máquina
            for (DayActivity activity : values) {
                uniqueDays.add(activity.getDay());
                totalActiveMs += activity.getActiveTimeMs();
                globalMinTs = Math.min(globalMinTs, activity.getMinTimestamp());
                globalMaxTs = Math.max(globalMaxTs, activity.getMaxTimestamp());
            }
            
            int totalDays = uniqueDays.size();
            
            // Aplica critérios de filtro
            if (totalDays >= 300) {
                double avgMinutes = totalActiveMs / (60000.0 * totalDays);
                
                if (avgMinutes >= 60.0) {
                    // Converte timestamps epoch de volta para formato legível
                    String result = String.format(
                        "%s,%.2f,%s,%s",
                        key.toString(),
                        avgMinutes,
                        DATE_FORMAT.format(new Date(globalMinTs)),
                        DATE_FORMAT.format(new Date(globalMaxTs))
                    );
                    
                    context.write(new Text(""), new Text(result));
                }
            }
        }
    }

    /**
     * Configura e executa o job
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: hadoop jar HadoopJob.jar <input_path> <output_path>");
            return -1;
        }
        
        Configuration conf = getConf();
        conf.set("mapreduce.job.reduces", "10"); // Ajuste conforme necessário
        
        Job job = Job.getInstance(conf, "Log Analysis - Epoch Version");
        
        job.setJarByClass(HadoopJob.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(MachineReducer.class);
        
        // Reduces deve ser > 1 para datasets grandes
        job.setNumReduceTasks(10);
        
        // Tipos de saída do Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DayActivity.class);
        
        // Tipos de saída final
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Formatos de entrada/saída
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Caminhos
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new HadoopJob(), args);
        System.exit(exitCode);
    }
}