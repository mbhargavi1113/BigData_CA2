import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Index extends Configured implements Tool {

    // ---------- JOB 1: count N (same idea as 2b) ----------
    public static class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t", -1);
            if (f.length == 3 && !f[0].trim().isEmpty()) ctx.getCounter("Docs", "TOTAL_DOCUMENTS").increment(1);
        }
    }

    // ---------- JOB 2 MAPPER ----------
    static class TermInfo { int tf; int[] pos = new int[8]; int n; }

    public static class IndexMapper extends Mapper<Object, Text, Text, PostingWritable> {
        private final Set<String> stop = new HashSet<String>();
        private final Text outKey = new Text();
        private final PostingWritable outVal = new PostingWritable();

        protected void setup(Context ctx) throws IOException {
            PostingWritable.DELTA = ctx.getConfiguration().getBoolean("index.delta", true);
            BufferedReader br = new BufferedReader(new FileReader("stopwords"));
            String line;
            while ((line = br.readLine()) != null) { line = line.trim().toLowerCase(Locale.ROOT); if (!line.isEmpty()) stop.add(line); }
            br.close();
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t", -1);
            if (f.length < 3) return;
            long[] dropped = new long[2];
            List<String> title = new ArrayList<String>(), body = new ArrayList<String>();
            Tokenizer.tokenize(f[1], stop, title, dropped);
            Tokenizer.tokenize(f[2], stop, body, dropped);

            // for ONE document: term -> (tf, positions). Only this one document is kept in memory.
            Map<String, TermInfo> terms = new HashMap<String, TermInfo>();
            int pos = 0;
            for (String t : title) add(terms, t, pos++, 3);      // a title word counts 3 for tf
            for (String t : body)  add(terms, t, pos++, 1);      // a body word counts 1
            for (Map.Entry<String, TermInfo> e : terms.entrySet()) {
                TermInfo ti = e.getValue();
                outKey.set(e.getKey());
                outVal.set(f[0], ti.tf, ti.pos, ti.n);
                ctx.write(outKey, outVal);                       // ONE posting per (term, document)
            }
        }

        private static void add(Map<String, TermInfo> m, String t, int pos, int weight) {
            TermInfo ti = m.get(t);
            if (ti == null) { ti = new TermInfo(); m.put(t, ti); }
            ti.tf += weight;
            if (ti.n == ti.pos.length) ti.pos = Arrays.copyOf(ti.pos, ti.n * 2);
            ti.pos[ti.n++] = pos;
        }
    }

    // ---------- JOB 2 REDUCER ----------
    static class Entry { String docId; int tf; int[] pos; double tfidf; }

    public static class IndexReducer extends Reducer<Text, PostingWritable, Text, Text> {
        private long N;
        protected void setup(Context ctx) throws IOException {
            N = ctx.getConfiguration().getLong("total.docs", -1);      // N handed over by the driver
            if (N <= 0) throw new IOException("total.docs not set");
            PostingWritable.DELTA = ctx.getConfiguration().getBoolean("index.delta", true);
        }

        protected void reduce(Text term, Iterable<PostingWritable> values, Context ctx) throws IOException, InterruptedException {
            List<Entry> list = new ArrayList<Entry>();
            for (PostingWritable p : values) {                         // Hadoop re-uses p, so we COPY the data
                Entry e = new Entry();
                e.docId = p.docId; e.tf = p.tf; e.pos = Arrays.copyOf(p.positions, p.count);
                list.add(e);
            }
            int df = list.size();                                      // number of documents containing the term
            double idf = Math.log10((double) N / df);
            for (Entry e : list) e.tfidf = Math.round(e.tf * idf * 10000.0) / 10000.0;   // 4 decimals
            Collections.sort(list, new Comparator<Entry>() {
                public int compare(Entry a, Entry b) {
                    int c = Double.compare(b.tfidf, a.tfidf);          // tfidf high to low
                    return c != 0 ? c : a.docId.compareTo(b.docId);    // then doc_id A to Z
                }
            });
            StringBuilder sb = new StringBuilder();
            for (Entry e : list) {
                if (sb.length() > 0) sb.append(' ');
                sb.append(e.docId).append(':').append(e.tf).append(':').append(String.format(Locale.ROOT, "%.4f", e.tfidf))
                  .append(':').append(Arrays.toString(e.pos).replace(" ", ""));
            }
            ctx.getCounter("Index", "TERMS").increment(1);
            ctx.write(term, new Text(df + "\t" + sb));                 // term <TAB> df <TAB> postings
        }
    }

    // ---------- DRIVER ----------
    public int run(String[] args) throws Exception {
        // args: <corpus dir> <stopwords file> <output dir>
        Configuration conf = getConf();
        Path corpus = new Path(args[0]);

        Job j1 = Job.getInstance(conf, "Q2 job1: count N");
        j1.setJarByClass(Index.class);
        j1.setMapperClass(CountMapper.class);
        j1.setNumReduceTasks(0);
        j1.setOutputKeyClass(NullWritable.class);
        j1.setOutputValueClass(NullWritable.class);
        j1.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(j1, corpus);
        if (!j1.waitForCompletion(true)) return 1;
        long n = j1.getCounters().findCounter("Docs", "TOTAL_DOCUMENTS").getValue();
        System.out.println("Job 1 finished: N = " + n);
        conf.setLong("total.docs", n);                                 // pass N to job 2

        Job job = Job.getInstance(conf, "Q2 job2: positional index (delta=" + conf.getBoolean("index.delta", true) + ")");
        job.setJarByClass(Index.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);                       // NO combiner (see the answer text)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PostingWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        Path sw = new Path(args[1]);
        FileSystem fs = sw.getFileSystem(conf);
        job.addCacheFile(new URI(fs.makeQualified(sw).toUri().toString() + "#stopwords"));
        FileInputFormat.addInputPath(job, corpus);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Index(), args));
    }
}
