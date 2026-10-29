import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Phrase search that reads ONLY the index (never the raw corpus).
//   Job A: find the documents where the phrase words are at consecutive positions
//   Job B: rank them (most phrase occurrences first)
public class Phrase extends Configured implements Tool {

    // the phrase words, made with the SAME tokeniser as the index (lower-case, stop-words removed)
    static List<String> phraseWords(Configuration conf) throws IOException {
        Set<String> stop = new HashSet<String>();
        BufferedReader br = new BufferedReader(new FileReader("stopwords"));
        String line;
        while ((line = br.readLine()) != null) { line = line.trim().toLowerCase(Locale.ROOT); if (!line.isEmpty()) stop.add(line); }
        br.close();
        List<String> words = new ArrayList<String>();
        Tokenizer.tokenize(conf.get("phrase", ""), stop, words, new long[2]);
        return words;
    }

    // ---------- JOB A MAPPER: one INDEX line:  term <TAB> df <TAB> doc:tf:tfidf:[pos,pos] doc:...
    public static class IndexMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> words = new HashSet<String>();
        protected void setup(Context ctx) throws IOException { words.addAll(phraseWords(ctx.getConfiguration())); }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length < 3 || !words.contains(f[0])) return;          // only the lines of the phrase words
            for (String posting : f[2].split(" ")) {                      // doc:tf:tfidf:[1,2,3]
                String[] p = posting.split(":", 4);
                String positions = p[3].substring(1, p[3].length() - 1);  // remove [ ]
                ctx.write(new Text(p[0]), new Text(f[0] + ":" + positions));   // key = doc_id, value = word:positions
            }
        }
    }

    // ---------- JOB A REDUCER: all phrase words of ONE document meet here
    public static class PhraseReducer extends Reducer<Text, Text, Text, IntWritable> {
        private List<String> phrase;
        protected void setup(Context ctx) throws IOException { phrase = phraseWords(ctx.getConfiguration()); }

        protected void reduce(Text doc, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            Map<String, int[]> pos = new HashMap<String, int[]>();
            for (Text t : values) {
                String s = t.toString();
                int c = s.indexOf(':');
                String[] nums = s.substring(c + 1).split(",");
                int[] a = new int[nums.length];
                for (int i = 0; i < a.length; i++) a[i] = Integer.parseInt(nums[i]);
                pos.put(s.substring(0, c), a);
            }
            for (String w : phrase) if (!pos.containsKey(w)) return;     // a word is missing in this document

            // the phrase starts at p if word number i is at position p+i, for every i
            int occurrences = 0;
            for (int p : pos.get(phrase.get(0))) {
                boolean ok = true;
                for (int i = 1; i < phrase.size() && ok; i++)
                    ok = Arrays.binarySearch(pos.get(phrase.get(i)), p + i) >= 0;
                if (ok) occurrences++;
            }
            if (occurrences == 0) { ctx.getCounter("Phrase", "DOCS_WITH_ALL_WORDS_BUT_NOT_ADJACENT").increment(1); return; }
            ctx.getCounter("Phrase", "DOCS_WITH_PHRASE").increment(1);
            ctx.getCounter("Phrase", "PHRASE_OCCURRENCES").increment(occurrences);
            ctx.write(doc, new IntWritable(occurrences));
        }
    }

    // ---------- JOB B: ranking. Key = (big number - occurrences) so that normal sorting puts MOST occurrences first
    public static class RankMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");                    // doc_id <TAB> occurrences
            long occ = Long.parseLong(f[1]);
            ctx.write(new Text(String.format("%010d|%s", 9999999999L - occ, f[0])), new Text(f[0] + "\t" + f[1]));
        }
    }

    public static class RankReducer extends Reducer<Text, Text, Text, Text> {
        private int rank = 0;
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            for (Text v : values) {
                String[] f = v.toString().split("\t");
                ctx.write(new Text(String.valueOf(++rank)), new Text(f[0] + "\t" + f[1]));   // rank, doc_id, occurrences
            }
        }
    }

    public int run(String[] args) throws Exception {
        // args: <stopwords file> <index dir> <temp dir> <result dir>      and   -Dphrase="big data analytics"
        Configuration conf = getConf();
        if (conf.get("phrase") == null) { System.err.println("give -Dphrase=\"w1 w2 w3\""); return 2; }
        Path sw = new Path(args[0]);
        FileSystem fs = sw.getFileSystem(conf);
        URI swUri = new URI(fs.makeQualified(sw).toUri().toString() + "#stopwords");

        Job a = Job.getInstance(conf, "Q2e match: " + conf.get("phrase"));
        a.setJarByClass(Phrase.class);
        a.setMapperClass(IndexMapper.class);
        a.setReducerClass(PhraseReducer.class);
        a.setMapOutputKeyClass(Text.class);   a.setMapOutputValueClass(Text.class);
        a.setOutputKeyClass(Text.class);      a.setOutputValueClass(IntWritable.class);
        a.setNumReduceTasks(2);
        a.addCacheFile(swUri);
        FileInputFormat.addInputPath(a, new Path(args[1]));              // the INDEX is the only input
        FileOutputFormat.setOutputPath(a, new Path(args[2]));
        if (!a.waitForCompletion(true)) return 1;

        Job b = Job.getInstance(conf, "Q2e ranking");
        b.setJarByClass(Phrase.class);
        b.setMapperClass(RankMapper.class);
        b.setReducerClass(RankReducer.class);
        b.setMapOutputKeyClass(Text.class);   b.setMapOutputValueClass(Text.class);
        b.setOutputKeyClass(Text.class);      b.setOutputValueClass(Text.class);
        b.setNumReduceTasks(1);                                          // one reducer = one sorted list
        FileInputFormat.addInputPath(b, new Path(args[2]));
        FileOutputFormat.setOutputPath(b, new Path(args[3]));
        return b.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Phrase(), args));
    }
}
