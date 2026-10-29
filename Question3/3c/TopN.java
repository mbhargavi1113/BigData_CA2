import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
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

// Q3 Job 3: Top-5 products per (category, month) by net revenue.
public class TopN extends Configured implements Tool {

    static final int N = 5;

    static String money(long c) { return (c < 0 ? "-" : "") + Math.abs(c) / 100 + "." + String.format("%02d", Math.abs(c) % 100); }

    // one candidate product of a group
    static class Entry {
        String product; long cents; long units;
        Entry(String p, long c, long u) { product = p; cents = c; units = u; }
    }

    // THE ORDER: revenue high->low, then units high->low, then product_id A->Z.
    // The product_id at the end makes it a STRICT TOTAL ORDER: two different products are NEVER "equal".
    // (buggy = true leaves the product_id out, to show the classic TreeSet bug)
    static Comparator<Entry> order(final boolean buggy) {
        return new Comparator<Entry>() {
            public int compare(Entry a, Entry b) {
                int c = Long.compare(b.cents, a.cents);
                if (c != 0) return c;
                c = Long.compare(b.units, a.units);
                if (c != 0 || buggy) return c;
                return a.product.compareTo(b.product);
            }
        };
    }

    // keeps at most N entries: add, and if there are more than N, throw away the worst one
    static boolean addBounded(TreeSet<Entry> set, Entry e) {
        if (!set.add(e)) return false;                 // false = comparator said "same entry" (collision)
        if (set.size() > N) set.pollLast();
        return true;
    }

    // MAPPER: for each (category, month) keep only the best 5 seen by THIS mapper; send them in cleanup()
    public static class TopMapper extends Mapper<Object, Text, Text, Text> {
        private final Map<String, TreeSet<Entry>> groups = new HashMap<String, TreeSet<Entry>>();
        private boolean skipFile, buggy;

        protected void setup(Context ctx) {
            skipFile = !((FileSplit) ctx.getInputSplit()).getPath().getName().startsWith("part-");
            buggy = ctx.getConfiguration().getBoolean("topn.buggy", false);
        }

        protected void map(Object off, Text line, Context ctx) {
            if (skipFile) return;
            String[] f = line.toString().split("\t");      // category month product net_revenue net_units
            String g = f[0] + "\t" + f[1];
            TreeSet<Entry> set = groups.get(g);
            if (set == null) { set = new TreeSet<Entry>(order(buggy)); groups.put(g, set); }
            Entry e = new Entry(f[2], Long.parseLong(f[3].replace(".", "")), Long.parseLong(f[4]));
            if (!addBounded(set, e)) ctx.getCounter("TopN", "TIE_COLLISIONS_LOST_IN_MAPPER").increment(1);
        }

        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (Map.Entry<String, TreeSet<Entry>> g : groups.entrySet()) {
                ctx.getCounter("TopN", "GROUPS_IN_MAPPERS").increment(1);
                for (Entry e : g.getValue()) {
                    ctx.getCounter("TopN", "ENTRIES_EMITTED_IN_CLEANUP").increment(1);
                    ctx.write(new Text(g.getKey()), new Text(e.product + "\t" + e.cents + "\t" + e.units));
                }
            }
        }
    }

    // CUSTOM PARTITIONER: decides the reducer by CATEGORY only
    public static class CategoryPartitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            String category = key.toString().split("\t")[0];
            return (category.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    // REDUCER: gets candidates from all mappers for one group, keeps the best 5, writes them with rank
    public static class TopReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, String> names = new HashMap<String, String>();
        private boolean buggy;

        protected void setup(Context ctx) throws IOException {
            buggy = ctx.getConfiguration().getBoolean("topn.buggy", false);
            BufferedReader br = new BufferedReader(new FileReader("products"));     // from the Distributed Cache
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",", -1);
                if (f.length >= 3 && !f[0].equals("product_id")) names.put(f[0].trim(), f[1].trim());
            }
            br.close();
        }

        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            TreeSet<Entry> set = new TreeSet<Entry>(order(buggy));
            for (Text v : values) {
                String[] f = v.toString().split("\t");
                if (!addBounded(set, new Entry(f[0], Long.parseLong(f[1]), Long.parseLong(f[2]))))
                    ctx.getCounter("TopN", "TIE_COLLISIONS_LOST_IN_REDUCER").increment(1);
            }
            int rank = 0;
            for (Entry e : set) {
                rank++;
                String name = names.containsKey(e.product) ? names.get(e.product) : "(unknown)";
                ctx.write(new Text(key.toString() + "\t" + rank), new Text(e.product + "\t" + name + "\t" + money(e.cents) + "\t" + e.units));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q3 job3 top-5");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TopMapper.class);
        job.setPartitionerClass(CategoryPartitioner.class);
        job.setReducerClass(TopReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);                        // at least 3 reducers are required
        Path pr = new Path(args[1]);
        FileSystem fs = pr.getFileSystem(conf);
        job.addCacheFile(new URI(fs.makeQualified(pr).toUri().toString() + "#products"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TopN(), args));
    }
}
