import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Q4 (d), JOB 2: funnel per IST date. Input = the NORMAL session files of job 1 (bot files are skipped).
public class Funnel extends Configured implements Tool {

    // a "vector" of 5 numbers written as text:  sessions,home,product,cart,success
    static String[] add(Iterable<Text> values) {
        long[] c = new long[5];
        for (Text v : values) {
            String[] f = v.toString().split(",");
            for (int i = 0; i < 5; i++) c[i] += Long.parseLong(f[i]);
        }
        return new String[] { c[0] + "," + c[1] + "," + c[2] + "," + c[3] + "," + c[4] };
    }

    public static class FunnelMapper extends Mapper<Object, Text, Text, Text> {
        private boolean skip;
        private final Text key = new Text(), val = new Text();

        protected void setup(Context ctx) {
            // only part-r-* files: the bots-r-* files are skipped, so bot sessions never reach the funnel
            skip = !((FileSplit) ctx.getInputSplit()).getPath().getName().startsWith("part-");
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            if (skip) return;
            String[] f = line.toString().split("\t");     // user seq start end dur views distinct entry exit stage
            int stage = Integer.parseInt(f[9]);
            key.set(f[2].substring(0, 10));               // IST date of the session start
            // a session that reached stage 3 also reached stages 1 and 2
            val.set("1," + (stage >= 1 ? 1 : 0) + "," + (stage >= 2 ? 1 : 0) + "," + (stage >= 3 ? 1 : 0) + "," + (stage >= 4 ? 1 : 0));
            ctx.getCounter("Funnel", "SESSIONS_IN_FUNNEL").increment(1);
            ctx.write(key, val);
        }
    }

    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, new Text(add(values)[0]));
        }
    }

    public static class FunnelReducer extends Reducer<Text, Text, Text, Text> {
        private static String pct(long a, long b) { return b == 0 ? "NA" : String.format(Locale.ROOT, "%.2f", 100.0 * a / b); }

        protected void reduce(Text date, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String[] f = add(values)[0].split(",");
            long s = Long.parseLong(f[0]), h = Long.parseLong(f[1]), p = Long.parseLong(f[2]), c = Long.parseLong(f[3]), x = Long.parseLong(f[4]);
            // date | sessions | home | product | cart | success | home>product % | product>cart % | cart>success % | home>success %
            ctx.write(date, new Text(s + "\t" + h + "\t" + p + "\t" + c + "\t" + x + "\t" + pct(p, h) + "\t" + pct(c, p) + "\t" + pct(x, c) + "\t" + pct(x, h)));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Q4 job2 funnel per IST date");
        job.setJarByClass(Funnel.class);
        job.setMapperClass(FunnelMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(FunnelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Funnel(), args));
    }
}
