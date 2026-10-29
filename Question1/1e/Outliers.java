import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Outliers extends Configured implements Tool {

    // MAPPER: loads the 1b statistics from the Distributed Cache, then checks every reading
    public static class OutlierMapper extends Mapper<Object, Text, Text, Text> {
        private final Map<String, Double> limit = new HashMap<String, Double>();   // key -> mean + 2*sigma
        private final Text key = new Text();
        private final Text val = new Text();

        protected void setup(Context ctx) throws IOException {
            // the cache file "stats" appears as a normal local file in the working folder
            BufferedReader br = new BufferedReader(new FileReader("stats"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split("\t");            // city, meter, month, count, min, max, mean, sigma
                double mean = Double.parseDouble(f[6]), sigma = Double.parseDouble(f[7]);
                limit.put(f[0] + "\t" + f[1] + "\t" + f[2], mean + 2 * sigma);
            }
            br.close();
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split(",");       // meter, city, time, kwh
            String k = f[1] + "\t" + f[0] + "\t" + f[2].substring(0, 7);
            Double lim = limit.get(k);
            if (lim == null) { ctx.getCounter("Outliers", "NO_STATS_FOR_GROUP").increment(1); return; }
            int isOutlier = Double.parseDouble(f[3]) > lim ? 1 : 0;
            key.set(k);
            val.set("1\t" + isOutlier);                    // "one reading" and "is it an outlier"
            ctx.write(key, val);
        }
    }

    // REDUCER: add up readings and outliers, then the percentage
    public static class OutlierReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long total = 0, outliers = 0;
            for (Text v : values) {
                String[] f = v.toString().split("\t");
                total += Long.parseLong(f[0]);
                outliers += Long.parseLong(f[1]);
            }
            ctx.write(key, new Text(String.format(Locale.ROOT, "%d\t%d\t%.2f", outliers, total, 100.0 * outliers / total)));
        }
    }

    public int run(String[] args) throws Exception {
        // args: <clean data dir> <1b stats file> <output dir>
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q1e outliers");
        job.setJarByClass(Outliers.class);
        job.setMapperClass(OutlierMapper.class);
        job.setReducerClass(OutlierReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path stats = new Path(args[1]);
        FileSystem fs = stats.getFileSystem(conf);
        job.addCacheFile(new URI(fs.makeQualified(stats).toUri().toString() + "#stats"));   // Distributed Cache
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Outliers(), args));
    }
}
