import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Q3 Job 2: net revenue and net units per (category, order month, product).
// The category comes from products.csv through a REPLICATED JOIN (Distributed Cache + Mapper.setup()).
public class Revenue extends Configured implements Tool {

    static String money(long c) { return (c < 0 ? "-" : "") + Math.abs(c) / 100 + "." + String.format("%02d", Math.abs(c) % 100); }

    // our own Writable: (revenue in cents, units). Adding two of them is safe in any order, so a combiner can use it.
    public static class RevenueWritable implements Writable {
        long cents, units;
        void set(long cents, long units) { this.cents = cents; this.units = units; }
        void add(RevenueWritable o) { cents += o.cents; units += o.units; }
        public void write(DataOutput out) throws IOException { out.writeLong(cents); out.writeLong(units); }
        public void readFields(DataInput in) throws IOException { cents = in.readLong(); units = in.readLong(); }
    }

    public static class RevMapper extends Mapper<Object, Text, Text, RevenueWritable> {
        private final Map<String, String> category = new HashMap<String, String>();   // product_id -> category
        private boolean skipFile;
        private final Text key = new Text();
        private final RevenueWritable val = new RevenueWritable();

        protected void setup(Context ctx) throws IOException {
            // replicated join: load the small file products.csv into memory, once per mapper
            BufferedReader br = new BufferedReader(new FileReader("products"));      // the cache file
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",", -1);                                   // product_id, product_name, category
                if (f.length >= 3 && !f[0].equals("product_id")) category.put(f[0].trim(), f[2].trim());
            }
            br.close();
            // the 3a output folder also has orphan_returns-* files: we want only the "part-" files
            skipFile = !((FileSplit) ctx.getInputSplit()).getPath().getName().startsWith("part-");
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            if (skipFile) return;
            String[] f = line.toString().split("\t");   // order_id line month product qty returned net_qty unit_price net_revenue
            String cat = category.get(f[3]);
            if (cat == null) { cat = "UNMAPPED"; ctx.getCounter("Revenue", "UNMAPPED_PRODUCT_LINES").increment(1); }
            key.set(cat + "\t" + f[2] + "\t" + f[3]);                                // category, month of the ORDER, product
            val.set(Long.parseLong(f[8].replace(".", "")), Long.parseLong(f[6]));    // revenue in cents, net units
            ctx.write(key, val);
        }
    }

    // COMBINER: adds revenue and units early on the mapper side (safe: addition)
    public static class SumCombiner extends Reducer<Text, RevenueWritable, Text, RevenueWritable> {
        private final RevenueWritable acc = new RevenueWritable();
        protected void reduce(Text key, Iterable<RevenueWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.set(0, 0);
            for (RevenueWritable v : values) acc.add(v);
            ctx.write(key, acc);
        }
    }

    public static class SumReducer extends Reducer<Text, RevenueWritable, Text, Text> {
        private final RevenueWritable acc = new RevenueWritable();
        protected void reduce(Text key, Iterable<RevenueWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.set(0, 0);
            for (RevenueWritable v : values) acc.add(v);
            ctx.getCounter("Revenue", "GROUPS_WRITTEN").increment(1);
            ctx.write(key, new Text(money(acc.cents) + "\t" + acc.units));          // category month product net_revenue net_units
        }
    }

    public int run(String[] args) throws Exception {
        // args: <3a output dir> <products.csv> <output dir>
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q3 job2 net revenue");
        job.setJarByClass(Revenue.class);
        job.setMapperClass(RevMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RevenueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        Path pr = new Path(args[1]);
        FileSystem fs = pr.getFileSystem(conf);
        job.addCacheFile(new URI(fs.makeQualified(pr).toUri().toString() + "#products"));   // Distributed Cache
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Revenue(), args));
    }
}
