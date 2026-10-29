import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Stats extends Configured implements Tool {

    // OUR OWN WRITABLE: carries the 5 numbers. Hadoop uses write()/readFields() to move it around.
    public static class StatsWritable implements Writable {
        long count; double sum, sumSq, min, max;

        public StatsWritable() { reset(); }
        void reset() { count = 0; sum = 0; sumSq = 0; min = Double.MAX_VALUE; max = -Double.MAX_VALUE; }
        void addValue(double x) { count++; sum += x; sumSq += x * x; if (x < min) min = x; if (x > max) max = x; }
        void merge(StatsWritable o) { count += o.count; sum += o.sum; sumSq += o.sumSq; if (o.min < min) min = o.min; if (o.max > max) max = o.max; }

        public void write(DataOutput out) throws IOException {
            out.writeLong(count); out.writeDouble(sum); out.writeDouble(sumSq); out.writeDouble(min); out.writeDouble(max);
        }
        public void readFields(DataInput in) throws IOException {
            count = in.readLong(); sum = in.readDouble(); sumSq = in.readDouble(); min = in.readDouble(); max = in.readDouble();
        }
    }

    // MAPPER: one clean line -> key (city, meter, month), value = stats of that single reading
    public static class StatsMapper extends Mapper<Object, Text, Text, StatsWritable> {
        private final Text key = new Text();
        private final StatsWritable val = new StatsWritable();

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split(",");          // meter, city, time, kwh
            String month = f[2].substring(0, 7);               // "2026-03"
            key.set(f[1] + "\t" + f[0] + "\t" + month);
            val.reset();
            val.addValue(Double.parseDouble(f[3]));
            ctx.write(key, val);
        }
    }

    // COMBINER (optional, used in 1c): merges the 5 numbers early, on the mapper side
    public static class StatsCombiner extends Reducer<Text, StatsWritable, Text, StatsWritable> {
        private final StatsWritable acc = new StatsWritable();
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.reset();
            for (StatsWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    // REDUCER: merge everything for one group, then compute the final numbers
    public static class StatsReducer extends Reducer<Text, StatsWritable, Text, Text> {
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx) throws IOException, InterruptedException {
            StatsWritable acc = new StatsWritable();
            for (StatsWritable v : values) acc.merge(v);
            double mean = acc.sum / acc.count;
            double variance = acc.sumSq / acc.count - mean * mean;
            if (variance < 0) {                                // tiny negative value from rounding errors
                ctx.getCounter("Stats", "NEGATIVE_VARIANCE_CLAMPED").increment(1);
                variance = 0;
            }
            double sigma = Math.sqrt(variance);
            ctx.getCounter("Stats", "GROUPS_WRITTEN").increment(1);
            ctx.write(key, new Text(String.format(Locale.ROOT, "%d\t%.3f\t%.3f\t%.6f\t%.6f",
                    acc.count, acc.min, acc.max, mean, sigma)));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Q1b statistics");
        job.setJarByClass(Stats.class);
        job.setMapperClass(StatsMapper.class);
        if (getConf().getBoolean("use.combiner", false)) job.setCombinerClass(StatsCombiner.class);   // off for now
        job.setReducerClass(StatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Stats(), args));
    }
}
