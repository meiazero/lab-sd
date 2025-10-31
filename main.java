import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Uso: hadoop jar lab1.jar Main <input> <output> [numReducers]");
      System.exit(1);
    }

    JobConf conf = new JobConf(Main.class);
    conf.setJobName("tempo_maquina_filtrada");

  // Não força modo local; deixa o cluster fornecer as configs (YARN/HDFS)

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
