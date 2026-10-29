import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StatsWrong extends Configured implements Tool {

    // MAPPER: key = city, meter, month    value = the single kwh reading
    public static class M extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text key = new Text();
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split(",");            // meter, city, time, kwh
            key.set(f[1] + "\t" + f[0] + "\t" + f[2].substring(0, 7));
            ctx.write(key, new DoubleWritable(Double.parseDouble(f[3])));
        }
    }

    // WRONG COMBINER: sends only the AVERAGE of what this mapper saw (it forgets HOW MANY readings that was)
    public static class WrongCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx) throws IOException, InterruptedException {
            double sum = 0; long n = 0;
            for (DoubleWritable v : values) { sum += v.get(); n++; }
            ctx.write(key, new DoubleWritable(sum / n));
        }
    }

    // REDUCER: averages the values it receives (these are now averages, not readings)
    public static class R extends Reducer<Text, DoubleWritable, Text, Text> {
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx) throws IOException, InterruptedException {
            double sum = 0; long k = 0;
            StringBuilder parts = new StringBuilder();
            for (DoubleWritable v : values) {
                sum += v.get(); k++;
                if (parts.length() > 0) parts.append(";");
                parts.append(String.format(Locale.ROOT, "%.6f", v.get()));
            }
            // output: wrong mean, how many partial averages arrived, the partial averages
            ctx.write(key, new Text(String.format(Locale.ROOT, "%.6f\t%d\t%s", sum / k, k, parts)));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Q1d wrong combiner");
        job.setJarByClass(StatsWrong.class);
        job.setMapperClass(M.class);
        job.setCombinerClass(WrongCombiner.class);
        job.setReducerClass(R.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new StatsWrong(), args));
    }
}
