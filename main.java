public static void main(String[] args) throws Exception {

    JobConf conf = new JobConf(TempoPorMaquina.class); // sua classe
    conf.setJobName("tempo_maquina_filtrada");

    conf.setNumReduceTasks(Integer.parseInt(args[2]));

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