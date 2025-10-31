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
