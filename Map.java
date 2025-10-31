import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper didático: lê linhas do event-trace e emite, por máquina, apenas
 * os intervalos ATIVOS (event_type == 1), convertidos para milissegundos.
 *
 * Chave:   machineId (LongWritable)
 * Valor:   "startMs;endMs" (Text)
 */
public class Map extends MapReduceBase
    implements Mapper<LongWritable, Text, LongWritable, Text> {

  private final LongWritable k = new LongWritable();
  private final Text v = new Text();

  @Override
  public void map(LongWritable key, Text value,
                  OutputCollector<LongWritable, Text> output,
                  Reporter reporter) throws IOException {

    String line = value.toString().trim();
    if (line.isEmpty()) return;
    if (line.charAt(0) == '#') return; // ignora cabeçalho/comentário

    String[] tokens = line.split("\\s+");
    if (tokens.length < 5) return; // linha inválida

    try {
      long machineId = Long.parseLong(tokens[1]);
      int eventType = Integer.parseInt(tokens[2]);
      if (eventType != 1) return; // só intervalos ATIVOS

      double startSec = Double.parseDouble(tokens[3]);
      double endSec   = Double.parseDouble(tokens[4]);
      long startMs = (long) Math.floor(startSec * 1000.0);
      long endMs   = (long) Math.floor(endSec   * 1000.0);
      if (endMs < startMs) return; // dados inconsistentes

      k.set(machineId);
      v.set(startMs + ";" + endMs);
      output.collect(k, v);
    } catch (NumberFormatException ex) {
      // ignora linhas malformadas
    }
  }
}
public static class Map extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, Text> {

    private LongWritable k = new LongWritable();
    private Text v = new Text();

    public void map(LongWritable key, Text value,
                    OutputCollector<LongWritable, Text> output,
                    Reporter reporter) throws IOException {

        String[] tokens = value.toString().split("\\s");

        // pula linha de comentário
        if (tokens[0].charAt(0) != '#') {

            Long machine = new Long(tokens[1]);

            // parece que tokens[2] indica se deve contar
            if (tokens[2].equals("1")) {
                k.set(machine);
                // aqui no seu exemplo era: tokens[3] + ";" + tokens[4]
                v.set(tokens[3] + ";" + tokens[4]);
                output.collect(k, v);
            }
        }
    }
}
