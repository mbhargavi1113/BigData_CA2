import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Q3 Job 1: REDUCE-SIDE JOIN of orders.csv and returns.csv on (order_id, line_no)
public class Join extends Configured implements Tool {

    // money as whole cents (no rounding errors):  "1299.00" -> 129900
    static long cents(String s) { return new BigDecimal(s.trim()).setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact(); }
    static String money(long c) { return (c < 0 ? "-" : "") + Math.abs(c) / 100 + "." + String.format("%02d", Math.abs(c) % 100); }

    // MAPPER: the file name tells us if the line is an ORDER line or a RETURN line.
    // Both get the SAME key "order_id,line_no", so they meet in the reducer. We TAG the value with O or R.
    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isOrders;
        protected void setup(Context ctx) {
            String name = ((FileSplit) ctx.getInputSplit()).getPath().getName().toLowerCase();
            isOrders = name.startsWith("orders");
        }
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split(",", -1);
            if (f[0].equals("order_id") || f[0].equals("return_id")) return;         // header line
            if (isOrders) {   // order_id,line_no,order_ts,customer_id,product_id,qty,unit_price,status
                ctx.getCounter("Join", "ORDER_LINES_READ").increment(1);
                ctx.write(new Text(f[0] + "," + f[1]),
                          new Text("O\t" + f[2] + "\t" + f[4] + "\t" + f[5] + "\t" + cents(f[6]) + "\t" + f[7]));
            } else {          // return_id,order_id,line_no,qty_returned,return_ts
                ctx.getCounter("Join", "RETURN_LINES_READ").increment(1);
                ctx.write(new Text(f[1] + "," + f[2]), new Text("R\t" + f[0] + "\t" + f[3]));
            }
        }
    }

    // REDUCER: ONE order line and ALL its returns arrive together
    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        protected void setup(Context ctx) { mos = new MultipleOutputs<NullWritable, Text>(ctx); }
        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }

        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            boolean haveOrder = false;
            String ts = "", product = "", status = "";
            long qty = 0, priceCents = 0, returned = 0, nReturns = 0;
            List<String> returnRows = new ArrayList<String>();            // only the returns of ONE order line (few)
            for (Text t : values) {
                String[] p = t.toString().split("\t");
                if (p[0].equals("O")) {
                    haveOrder = true; ts = p[1]; product = p[2]; qty = Long.parseLong(p[3]); priceCents = Long.parseLong(p[4]); status = p[5];
                } else {
                    nReturns++; returned += Long.parseLong(p[2]);
                    returnRows.add(p[1] + "," + key + "," + p[2]);
                }
            }
            if (!haveOrder) {                                              // returns that match NO order line
                ctx.getCounter("Join", "ORPHAN_RETURNS").increment(nReturns);
                for (String r : returnRows) mos.write("orphanreturns", NullWritable.get(), new Text(r), "orphan_returns");
                return;
            }
            if (status.equals("CANCELLED")) {                              // exclude cancelled lines
                ctx.getCounter("Join", "CANCELLED_LINES_EXCLUDED").increment(1);
                return;
            }
            if (nReturns > 1) ctx.getCounter("Join", "LINES_WITH_MULTIPLE_RETURNS").increment(1);
            long net = qty - returned;
            if (net < 0) { net = 0; ctx.getCounter("Join", "RETURN_EXCEEDS_ORDER").increment(1); }   // returned more than ordered: net = 0
            String[] k = key.toString().split(",");
            ctx.getCounter("Join", "NET_LINES_WRITTEN").increment(1);
            // order_id, line_no, month of the ORDER, product, qty, returned, net_qty, unit_price, net_revenue
            ctx.write(NullWritable.get(), new Text(k[0] + "\t" + k[1] + "\t" + ts.substring(0, 7) + "\t" + product + "\t" + qty + "\t" + returned
                    + "\t" + net + "\t" + money(priceCents) + "\t" + money(net * priceCents)));
        }
    }

    public int run(String[] args) throws Exception {
        // args: <orders.csv> <returns.csv> <output dir>
        Job job = Job.getInstance(getConf(), "Q3 job1 join");
        job.setJarByClass(Join.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        MultipleOutputs.addNamedOutput(job, "orphanreturns", TextOutputFormat.class, NullWritable.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Join(), args));
    }
}
