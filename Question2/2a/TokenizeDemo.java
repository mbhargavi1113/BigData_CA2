import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TokenizeDemo extends Configured implements Tool {

    // MAP-ONLY job: prints the tokens of every document with their positions
    // term*@0 = token from the TITLE,  term@5 = token from the BODY
    public static class TokMapper extends Mapper<Object, Text, NullWritable, Text> {
        private final Set<String> stop = new HashSet<String>();

        protected void setup(Context ctx) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader("stopwords"));   // Distributed Cache file
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim().toLowerCase(Locale.ROOT);
                if (!line.isEmpty()) stop.add(line);
            }
            br.close();
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t", -1);        // -1 keeps an EMPTY body
            if (f.length < 3) return;
            ctx.getCounter("Tokens", "DOCUMENTS").increment(1);
            if (f[2].trim().isEmpty()) ctx.getCounter("Tokens", "EMPTY_BODY_DOCUMENTS").increment(1);

            long[] dropped = new long[2];
            List<String> title = new ArrayList<String>(), body = new ArrayList<String>();
            Tokenizer.tokenize(f[1], stop, title, dropped);       // title first ...
            Tokenizer.tokenize(f[2], stop, body, dropped);        // ... then body: ONE stream
            ctx.getCounter("Tokens", "TOKENS_KEPT").increment(title.size() + body.size());
            ctx.getCounter("Tokens", "TOKENS_DROPPED_TOO_SHORT").increment(dropped[0]);
            ctx.getCounter("Tokens", "TOKENS_DROPPED_STOPWORD").increment(dropped[1]);

            StringBuilder sb = new StringBuilder();
            int pos = 0;
            for (String t : title) sb.append(t).append("*@").append(pos++).append(' ');
            for (String t : body)  sb.append(t).append('@').append(pos++).append(' ');
            ctx.write(NullWritable.get(), new Text(f[0] + "\t" + sb.toString().trim()));
        }
    }

    public int run(String[] args) throws Exception {
        // args: <corpus dir> <stopwords file> <output dir>
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q2a tokenise");
        job.setJarByClass(TokenizeDemo.class);
        job.setMapperClass(TokMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        Path sw = new Path(args[1]);
        FileSystem fs = sw.getFileSystem(conf);
        job.addCacheFile(new URI(fs.makeQualified(sw).toUri().toString() + "#stopwords"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TokenizeDemo(), args));
    }
}

