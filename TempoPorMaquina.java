import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Driver do job Hadoop (API mapred) para calcular o tempo médio por dia
 * (300 dias) e filtrar máquinas com média >= 1h/dia.
 *
 * Uso:
 *   hadoop jar lab1.jar TempoPorMaquina <input> <output> [numReducers]
 */
public class TempoPorMaquina {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Uso: hadoop jar lab1.jar TempoPorMaquina <input> <output> [numReducers]");
      System.exit(1);
    }

    JobConf conf = new JobConf(TempoPorMaquina.class);
    conf.setJobName("tempo_maquina_filtrada");

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    if (args.length >= 3) {
      conf.setNumReduceTasks(Integer.parseInt(args[2]));
    }

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
