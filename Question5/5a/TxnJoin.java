import java.io.IOException;
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

// Q5 JOB 1: reduce-side join   gateway_txns (ONE)  x  bank_settlements (MANY)   on txn_id
public class TxnJoin extends Configured implements Tool {

    public static class JoinMapper extends Mapper<Object, Text, Tagged.Key, Text> {
        private boolean txns, skip;
        private final Tagged.Key key = new Tagged.Key();
        private final Text val = new Text();

        protected void setup(Context ctx) {
            String name = ((FileSplit) ctx.getInputSplit()).getPath().getName().toLowerCase();
            txns = name.startsWith("gateway_txns");
            skip = !txns && !name.startsWith("bank_settlements");
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            if (skip) return;
            String[] f = line.toString().split(",", -1);
            if (f[0].equals("txn_id") || f[0].equals("settlement_id")) { ctx.getCounter("Job1", "HEADER_LINES").increment(1); return; }
            try {
                if (txns) {                       // txn_id,order_id,amount,currency,status,txn_ts
                    ctx.getCounter("Job1", "TXN_LINES").increment(1);
                    if (f.length != 6) throw new IllegalArgumentException();
                    key.set(f[0], 0, "");                                                                  // tag 0 = the ONE side
                    val.set("T\t" + f[1] + "\t" + Tagged.toCents(f[2]) + "\t" + f[3] + "\t" + f[4] + "\t" + f[5]);
                } else {                          // settlement_id,txn_id,settled_amount_inr,settlement_date
                    ctx.getCounter("Job1", "SETTLEMENT_LINES").increment(1);
                    if (f.length != 4) throw new IllegalArgumentException();
                    key.set(f[1], 1, f[3] + " " + f[0]);                                                   // tag 1 = the MANY side
                    val.set("S\t" + f[0] + "\t" + Tagged.toCents(f[2]) + "\t" + f[3]);
                }
                ctx.write(key, val);
            } catch (RuntimeException e) {
                ctx.getCounter("Job1", "MALFORMED_LINES").increment(1);
            }
        }
    }

    public static class JoinReducer extends Reducer<Tagged.Key, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private final Text out = new Text();

        protected void setup(Context ctx) { mos = new MultipleOutputs<NullWritable, Text>(ctx); }
        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }

        protected void reduce(Tagged.Key key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String txnId = key.id.toString();
            String txnPart = null;                 // the ONE side: a single record
            long settled = 0, count = 0;           // the MANY side is only SUMMED, never stored in a list
            for (Text v : values) {
                String s = v.toString();
                if (s.charAt(0) == 'T') {
                    if (txnPart != null) { ctx.getCounter("Job1", "DUPLICATE_TXN_ID").increment(1); continue; }
                    txnPart = s.substring(2);
                } else {
                    String[] f = s.split("\t");
                    long cents = Long.parseLong(f[2]);
                    if (txnPart == null) {         // a settlement that arrived WITHOUT a transaction in front of it = ORPHAN_SETTLEMENT
                        ctx.getCounter("Job1", "ORPHAN_SETTLEMENTS").increment(1);
                        out.set("ORPHAN_SETTLEMENT\t" + f[1] + "\tINR\t" + Tagged.format(cents) + "\t0.00\t0.00\t" + Tagged.format(cents)
                                + "\t0\t0\t0\t0\t0.00\ttxn_id=" + txnId + " settlement_date=" + f[3]);
                        mos.write("cat", NullWritable.get(), out, "ORPHAN_SETTLEMENT/part");
                    } else { settled += cents; count++; }
                }
            }
            if (txnPart == null) return;           // only orphans in this group
            ctx.getCounter("Job1", "TXNS_WRITTEN").increment(1);
            if (count == 0) ctx.getCounter("Job1", "TXNS_WITH_NO_SETTLEMENT").increment(1);
            if (count > 1) ctx.getCounter("Job1", "TXNS_SETTLED_MORE_THAN_ONCE").increment(1);
            if (count > ctx.getCounter("Job1", "MAX_SETTLEMENTS_FOR_ONE_TXN").getValue()) ctx.getCounter("Job1", "MAX_SETTLEMENTS_FOR_ONE_TXN").setValue(count);
            // txn_id, order_id, amount_cents, currency, status, txn_ts, settled_cents, settlement_count
            out.set(txnId + "\t" + txnPart + "\t" + settled + "\t" + count);
            ctx.write(NullWritable.get(), out);
        }
    }

    public int run(String[] args) throws Exception {
        // args: <gateway_txns.csv> <bank_settlements.csv> <output dir>
        Job job = Job.getInstance(getConf(), "Q5 job1 txns x settlements");
        job.setJarByClass(TxnJoin.class);
        job.setMapperClass(JoinMapper.class);
        job.setPartitionerClass(Tagged.Part.class);
        job.setSortComparatorClass(Tagged.SortCmp.class);
        job.setGroupingComparatorClass(Tagged.GroupCmp.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Tagged.Key.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        MultipleOutputs.addNamedOutput(job, "cat", TextOutputFormat.class, NullWritable.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TxnJoin(), args));
    }
}
