import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountDocs extends Configured implements Tool {

    // MAPPER: +1 on a custom counter for every document line. It writes nothing.
    public static class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t", -1);            // doc_id, title, body
            if (f.length == 3 && !f[0].trim().isEmpty())
                ctx.getCounter("Docs", "TOTAL_DOCUMENTS").increment(1);
            else
                ctx.getCounter("Docs", "MALFORMED_LINES").increment(1);
            if (f.length == 3 && f[2].trim().isEmpty())
                ctx.getCounter("Docs", "EMPTY_BODY_DOCUMENTS").increment(1);   // still a document!
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Q2b count N");
        job.setJarByClass(CountDocs.class);
        job.setMapperClass(CountMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);            // no output files, only the counter
        FileInputFormat.addInputPath(job, new Path(args[0]));
        if (!job.waitForCompletion(true)) return 1;

        // the driver reads the counter value = N
        long n = job.getCounters().findCounter("Docs", "TOTAL_DOCUMENTS").getValue();
        System.out.println("=================================");
        System.out.println("N (total documents) = " + n);
        System.out.println("=================================");
        FileWriter fw = new FileWriter("N.txt"); fw.write(n + "\n"); fw.close();

        // HOW N IS PASSED TO JOB 2 (we do this for real in 2c):
        //   Configuration conf2 = new Configuration();
        //   conf2.setLong("total.docs", n);                     // driver
        //   long N = context.getConfiguration().getLong("total.docs", 0);   // inside Job 2
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CountDocs(), args));
    }
}

