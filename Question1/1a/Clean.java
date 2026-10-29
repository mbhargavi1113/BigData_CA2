import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Clean extends Configured implements Tool {

    // MAPPER: looks at ONE line. Bad line -> count it and throw it away.
    public static class CleanMapper extends Mapper<Object, Text, Text, Text> {
        static final DateTimeFormatter F =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            ctx.getCounter("Quality", "INPUT_LINES").increment(1);
            String[] f = line.toString().split(",", -1);
            if (f.length != 4) { ctx.getCounter("Quality", "REJECT_WRONG_FIELD_COUNT").increment(1); return; }
            String meter = f[0].trim(), city = f[1].trim(), ts = f[2].trim(), kwhText = f[3].trim();
            if (meter.isEmpty() || city.isEmpty()) { ctx.getCounter("Quality", "REJECT_MISSING_METER_OR_CITY").increment(1); return; }
            try { LocalDateTime.parse(ts, F); }
            catch (Exception e) { ctx.getCounter("Quality", "REJECT_BAD_TIMESTAMP").increment(1); return; }
            double kwh;
            try { kwh = Double.parseDouble(kwhText); }
            catch (NumberFormatException e) { ctx.getCounter("Quality", "REJECT_NON_NUMERIC_KWH").increment(1); return; }
            if (Double.isNaN(kwh) || Double.isInfinite(kwh)) { ctx.getCounter("Quality", "REJECT_NON_NUMERIC_KWH").increment(1); return; }
            if (kwh < 0) { ctx.getCounter("Quality", "REJECT_NEGATIVE_KWH").increment(1); return; }
            // key = meter + time, so duplicates meet in the same reducer
            ctx.write(new Text(meter + "," + ts), new Text(city + "," + kwhText));
        }
    }

    // REDUCER: gets all lines with the same meter+time. Keep ONE, count the rest.
    public static class DedupReducer extends Reducer<Text, Text, NullWritable, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            TreeSet<String> different = new TreeSet<String>();   // different versions of the same reading
            int total = 0;
            for (Text v : values) { total++; different.add(v.toString()); }
            ctx.getCounter("Quality", "REJECT_DUPLICATE_EXACT").increment(total - different.size());
            ctx.getCounter("Quality", "REJECT_DUPLICATE_CONFLICTING").increment(different.size() - 1);
            String keep = different.first();                      // always the same choice
            String[] mt = key.toString().split(",", 2);           // meter , time
            String[] ck = keep.split(",", 2);                     // city , kwh
            ctx.getCounter("Quality", "CLEAN_RECORDS_WRITTEN").increment(1);
            ctx.write(NullWritable.get(), new Text(mt[0] + "," + ck[0] + "," + mt[1] + "," + ck[1]));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Q1a clean");
        job.setJarByClass(Clean.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(DedupReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Clean(), args));
    }
}
