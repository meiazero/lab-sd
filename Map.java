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

