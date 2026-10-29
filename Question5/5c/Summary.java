import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Q5 (c): SUMMARY file. For every category: how many rows, and the INR totals.
// Input line (tab separated): category, id, currency, amount, order_amount_inr, net_paid_inr, settled_inr, ...
public class Summary extends Configured implements Tool {

    static long toCents(String s) { return new java.math.BigDecimal(s.trim()).movePointRight(2).longValueExact(); }
    static String format(long c) { return (c < 0 ? "-" : "") + Math.abs(c) / 100 + "." + String.format("%02d", Math.abs(c) % 100); }

    public static class SumMapper extends Mapper<Object, Text, Text, Text> {
        private final Text k = new Text(), v = new Text();
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            k.set(f[0]);                                                            // the category
            v.set("1\t" + toCents(f[4]) + "\t" + toCents(f[5]) + "\t" + toCents(f[6]));    // count, order_inr, net_paid_inr, settled_inr (in paise)
            ctx.write(k, v);
        }
    }

    // COMBINER: adds up on the mapper side, keeps the numbers in paise (the output stays in the same format as the input)
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long n = 0, a = 0, b = 0, c = 0;
            for (Text v : values) { String[] f = v.toString().split("\t"); n += Long.parseLong(f[0]); a += Long.parseLong(f[1]); b += Long.parseLong(f[2]); c += Long.parseLong(f[3]); }
            ctx.write(key, new Text(n + "\t" + a + "\t" + b + "\t" + c));
        }
    }

    // REDUCER: the final totals, written as normal money
    public static class SumReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long n = 0, a = 0, b = 0, c = 0;
            for (Text v : values) { String[] f = v.toString().split("\t"); n += Long.parseLong(f[0]); a += Long.parseLong(f[1]); b += Long.parseLong(f[2]); c += Long.parseLong(f[3]); }
            ctx.write(key, new Text(n + "\t" + format(a) + "\t" + format(b) + "\t" + format(c)));
        }
    }

    public int run(String[] args) throws Exception {
        // args: <output dir> <by_category dir>
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q5 summary per category");
        job.setJarByClass(Summary.class);
        job.setMapperClass(SumMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        Path root = new Path(args[1]);
        FileSystem fs = root.getFileSystem(conf);
        for (FileStatus s : fs.listStatus(root))               // every category folder is an input
            if (s.isDirectory()) FileInputFormat.addInputPath(job, s.getPath());
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Summary(), args));
    }
}
