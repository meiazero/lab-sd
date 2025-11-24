import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ActiveTimeAggregator
 * Single-file MapReduce job:
 * - Mapper: emits (node_name, "start,end") for event_type==1
 * - Reducer: merges intervals, splits across UTC-days, sums active ms,
 *   filters nodes with total_active_ms >= 300 days, computes avg per day,
 *   emits CSV: node_name,avg_HH:MM:SS,global_start_ms,global_end_ms
 *
 * Assumes input CSV with header and fields:
 * component_id,node_name,event_type,event_start_time,event_end_time
 * where start/end are epoch milliseconds (long).
 * Version made with ChatGPT 5-mini: https://chat.com
 */
public class ActiveTimeAggregator {

    public static final long MS_PER_DAY = 24L * 60 * 60 * 1000;
    public static final long THRESHOLD_DAYS = 300L;
    public static final long THRESHOLD_MS = THRESHOLD_DAYS * MS_PER_DAY;
    public static final long ONE_HOUR_MS = 60L * 60 * 1000;

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            // skip header if present
            if (line.startsWith("component_id") || line.startsWith("componentId")) return;

            // CSV splitting - simple split, input is clean per user statement
            // format: component_id,node_name,event_type,event_start_time,event_end_time
            String[] parts = line.split(",", -1);
            if (parts.length < 5) return;
            String node = parts[1].trim();
            String eventType = parts[2].trim();
            String startS = parts[3].trim();
            String endS = parts[4].trim();
            if (!"1".equals(eventType)) return; // only active intervals

            // sanity parse
            try {
                long s = (long) (Double.parseDouble(startS) * 1000);
                long e = (long) (Double.parseDouble(endS) * 1000);
                if (e <= s) return;
                outKey.set(node);
                outVal.set(s + "," + e);
                context.write(outKey, outVal);
            } catch (NumberFormatException ignored) {
            }
        }
    }

    public static class MergeReduce extends Reducer<Text, Text, Text, Text> {
        private Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // collect intervals
            List<long[]> intervals = new ArrayList<>();
            for (Text v : values) {
                String[] p = v.toString().split(",", 2);
                if (p.length < 2) continue;
                try {
                    long s = Long.parseLong(p[0]);
                    long e = Long.parseLong(p[1]);
                    if (e > s) intervals.add(new long[]{s, e});
                } catch (NumberFormatException ignored) { }
            }
            if (intervals.isEmpty()) return;

            // sort by start
            Collections.sort(intervals, (a, b) -> Long.compare(a[0], b[0]));

            // merge overlapping/adjacent intervals
            List<long[]> merged = new ArrayList<>();
            long curS = intervals.get(0)[0];
            long curE = intervals.get(0)[1];
            for (int i = 1; i < intervals.size(); ++i) {
                long s = intervals.get(i)[0];
                long e = intervals.get(i)[1];
                if (s <= curE) {
                    // overlap or contiguous
                    if (e > curE) curE = e;
                } else {
                    merged.add(new long[]{curS, curE});
                    curS = s;
                    curE = e;
                }
            }
            merged.add(new long[]{curS, curE});

            // Calculate global start, end, and total active time
            long globalStart = Long.MAX_VALUE;
            long globalEnd = Long.MIN_VALUE;
            long totalActiveMs = 0L;

            for (long[] in : merged) {
                long s = in[0];
                long e = in[1];
                if (s < globalStart) globalStart = s;
                if (e > globalEnd) globalEnd = e;
                totalActiveMs += (e - s);
            }

            // Check 300-day lifespan threshold
            long lifespan = globalEnd - globalStart;
            if (lifespan < THRESHOLD_MS) return;

            // Calculate average active time per day based on lifespan
            double days = (double) lifespan / MS_PER_DAY;
            if (days < 1.0) days = 1.0; // Safety check
            
            double avgPerDayMs = totalActiveMs / days;

            if (avgPerDayMs < ONE_HOUR_MS) return; 

            // format avgPerDayMs to HH:MM:SS
            String avgHms = msToHMS((long) avgPerDayMs);

            // output CSV: node_name,avg_HH:MM:SS,global_start_epoch_ms,global_end_epoch_ms
            StringBuilder sb = new StringBuilder();
            sb.append(avgHms).append(",").append(globalStart).append(",").append(globalEnd);
            outVal.set(sb.toString());
            context.write(key, outVal);
        }

        private String msToHMS(long ms) {
            long totalSec = ms / 1000;
            long hours = totalSec / 3600;
            long minutes = (totalSec % 3600) / 60;
            long seconds = totalSec % 60;
            return String.format("%02d:%02d:%02d", hours, minutes, seconds);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ActiveTimeAggregator <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Active Time Aggregator");
        job.setJarByClass(ActiveTimeAggregator.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MergeReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // allow Hadoop to choose reducers automatically (number depends on cluster)
        // don't explicitly setNumReduceTasks()

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
