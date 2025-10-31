import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer didático: soma o tempo ativo total por máquina (em ms),
 * guarda o menor início e maior fim entre os intervalos ativos e
 * emite apenas as máquinas cuja média por dia (em 300 dias) é >= 1 hora.
 *
 * Chave de saída: machineId
 * Valor de saída: "HH:mm:ss;inicio_iso_utc;fim_iso_utc"
 */
public class Reduce extends MapReduceBase
    implements Reducer<LongWritable, Text, LongWritable, Text> {

  private static final int NUM_DIAS = 300;
  private static final long UMA_HORA_MS = 60L * 60L * 1000L;

  private static final DateTimeFormatter ISO_UTC =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
          .withLocale(Locale.ROOT)
          .withZone(ZoneId.of("UTC"));

  @Override
  public void reduce(LongWritable key,
                     Iterator<Text> values,
                     OutputCollector<LongWritable, Text> output,
                     Reporter reporter) throws IOException {

    long sumMs = 0L;
    long traceStartMs = Long.MAX_VALUE;
    long traceEndMs   = Long.MIN_VALUE;

    while (values.hasNext()) {
      String[] parts = values.next().toString().split(";");
      if (parts.length < 2) continue;
      try {
        long startMs = Long.parseLong(parts[0]);
        long endMs   = Long.parseLong(parts[1]);
        if (endMs < startMs) continue;

        sumMs += (endMs - startMs);
        if (startMs < traceStartMs) traceStartMs = startMs;
        if (endMs   > traceEndMs)   traceEndMs   = endMs;
      } catch (NumberFormatException ex) {
        // ignora valores inválidos
      }
    }

    if (sumMs <= 0) return;

    long mediaPorDiaMs = sumMs / NUM_DIAS;
    if (mediaPorDiaMs >= UMA_HORA_MS) {
      String mediaFmt = formatDuration(mediaPorDiaMs);
      String inicio = traceStartMs == Long.MAX_VALUE ? "-" : ISO_UTC.format(Instant.ofEpochMilli(traceStartMs));
      String fim    = traceEndMs   == Long.MIN_VALUE ? "-" : ISO_UTC.format(Instant.ofEpochMilli(traceEndMs));
      output.collect(key, new Text(mediaFmt + ";" + inicio + ";" + fim));
    }
  }

  // HH:mm:ss a partir de milissegundos
  private static String formatDuration(long ms) {
    long totalSeconds = ms / 1000L;
    long hours = totalSeconds / 3600L;
    long minutes = (totalSeconds % 3600L) / 60L;
    long seconds = totalSeconds % 60L;
    return String.format(Locale.ROOT, "%02d:%02d:%02d", hours, minutes, seconds);
  }
}
public static class Reduce extends MapReduceBase
        implements Reducer<LongWritable, Text, LongWritable, Text> {

    // constantes do exercício
    private static final int NUM_DIAS = 300;               // 300 dias de atividade
    private static final long UMA_HORA_MS = 60L * 60L * 1000L;  // 3_600_000 ms

    public void reduce(LongWritable key,
                       Iterator<Text> values,
                       OutputCollector<LongWritable, Text> output,
                       Reporter reporter) throws IOException {

        long sum = 0L;          // soma total de tempo da máquina
        long traceStart = Long.MAX_VALUE;  // menor início
        long traceEnd   = Long.MIN_VALUE;  // maior fim

        while (values.hasNext()) {
            String line = values.next().toString();
            String[] tokens = line.split(";");
            long start = new Double(tokens[0]).longValue();
            long end   = new Double(tokens[1]).longValue();

            // atualiza menor início e maior fim
            if (start < traceStart) {
                traceStart = start;
            }
            if (end > traceEnd) {
                traceEnd = end;
            }

            // soma tempo deste intervalo
            sum += (end - start);
        }

        // calcula média por dia
        long mediaPorDia = sum / NUM_DIAS;  // ainda em ms

        // regra do exercício 2: só emite se média >= 1h
        if (mediaPorDia >= UMA_HORA_MS) {
            // vamos montar saída: "<mediaMs>;<inicio>;<fim>"
            // você pode formatar para horas se quiser
            Text val = new Text(
                    mediaPorDia + ";" + traceStart + ";" + traceEnd
            );
            output.collect(key, val);
        }
        // senão, não emite nada (a máquina é descartada)
    }
}
