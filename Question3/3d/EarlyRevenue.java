import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Q3d: the WRONG idea. Job 2 with "Top-5" already inside the mapper.
// Each mapper adds up the revenue of ITS OWN lines, then keeps only its 5 best products per (category, month).
public class EarlyRevenue extends Configured implements Tool {

    static String money(long c) { return (c < 0 ? "-" : "") + Math.abs(c) / 100 + "." + String.format("%02d", Math.abs(c) % 100); }

    public static class EarlyMapper extends Mapper<Object, Text, Text, Text> {
        private final Map<String, String> category = new HashMap<String, String>();
        // partial totals of THIS mapper only:  "category TAB month TAB product" -> {cents, units}
        private final Map<String, long[]> partial = new HashMap<String, long[]>();
        private boolean skipFile;

        protected void setup(Context ctx) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader("products"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",", -1);
                if (f.length >= 3 && !f[0].equals("product_id")) category.put(f[0].trim(), f[2].trim());
            }
            br.close();
            skipFile = !((FileSplit) ctx.getInputSplit()).getPath().getName().startsWith("part-");
        }

        protected void map(Object off, Text line, Context ctx) {
            if (skipFile) return;
            String[] f = line.toString().split("\t");
            String cat = category.get(f[3]);
            if (cat == null) cat = "UNMAPPED";
            String k = cat + "\t" + f[2] + "\t" + f[3];
            long[] t = partial.get(k);
            if (t == null) { t = new long[2]; partial.put(k, t); }
            t[0] += Long.parseLong(f[8].replace(".", ""));
            t[1] += Long.parseLong(f[6]);
        }

        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            // group the partial totals by (category, month)
            Map<String, List<String>> byGroup = new HashMap<String, List<String>>();
            for (String k : partial.keySet()) {
                String[] p = k.split("\t");
                String g = p[0] + "\t" + p[1];
                if (!byGroup.containsKey(g)) byGroup.put(g, new ArrayList<String>());
                byGroup.get(g).add(k);
            }
            for (List<String> keys : byGroup.values()) {
                final Map<String, long[]> pm = partial;
                Collections.sort(keys, new Comparator<String>() {          // best partial revenue first
                    public int compare(String a, String b) {
                        int c = Long.compare(pm.get(b)[0], pm.get(a)[0]);
                        return c != 0 ? c : a.compareTo(b);
                    }
                });
                for (int i = 0; i < keys.size(); i++) {
                    if (i >= 5) { ctx.getCounter("Early", "PRODUCTS_THROWN_AWAY_IN_MAPPER").increment(1); continue; }   // <-- THE MISTAKE
                    long[] t = pm.get(keys.get(i));
                    ctx.write(new Text(keys.get(i)), new Text(t[0] + "\t" + t[1]));
                }
            }
        }
    }

    // the reducer is the normal one: it adds up what it receives
    public static class SumReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long cents = 0, units = 0;
            for (Text v : values) {
                String[] f = v.toString().split("\t");
                cents += Long.parseLong(f[0]);
                units += Long.parseLong(f[1]);
            }
            ctx.write(key, new Text(money(cents) + "\t" + units));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q3d early top-5 (WRONG)");
        job.setJarByClass(EarlyRevenue.class);
        job.setMapperClass(EarlyMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        Path pr = new Path(args[1]);
        FileSystem fs = pr.getFileSystem(conf);
        job.addCacheFile(new URI(fs.makeQualified(pr).toUri().toString() + "#products"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new EarlyRevenue(), args));
    }
}
