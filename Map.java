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
