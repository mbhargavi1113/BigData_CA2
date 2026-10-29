import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

// Q5 JOB 2: reduce-side join   orders (ONE)  x  transaction lines from job 1 (MANY)   on order_id,  then the classification.
public class Reconcile extends Configured implements Tool {

    static final long TOLERANCE_PAISE = 100;      // INR 1.00

    public static class ReconMapper extends Mapper<Object, Text, Tagged.Key, Text> {
        private boolean orders, skip;
        private final Tagged.Key key = new Tagged.Key();
        private final Text val = new Text();

        protected void setup(Context ctx) {
            String name = ((FileSplit) ctx.getInputSplit()).getPath().getName().toLowerCase();
            orders = name.startsWith("orders");
            skip = !orders && !name.startsWith("part-");
        }

        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            if (skip) return;
            try {
                if (orders) {                         // order_id,customer_id,order_amount,currency,order_ts
                    String[] f = line.toString().split(",", -1);
                    if (f[0].equals("order_id")) { ctx.getCounter("Job2", "HEADER_LINES").increment(1); return; }
                    ctx.getCounter("Job2", "ORDER_LINES").increment(1);
                    if (f.length != 5) throw new IllegalArgumentException();
                    key.set(f[0], 0, "");                                                  // tag 0 = the ONE side (order)
                    val.set("O\t" + Tagged.toCents(f[2]) + "\t" + f[3] + "\t" + f[4]);
                } else {                              // txn_id order_id amount_cents currency status txn_ts settled_cents settlement_count
                    String[] f = line.toString().split("\t");
                    ctx.getCounter("Job2", "TXN_SUMMARY_LINES").increment(1);
                    if (f.length != 8) throw new IllegalArgumentException();
                    key.set(f[1], 1, f[5] + " " + f[0]);                                   // tag 1 = the MANY side, in time order
                    val.set("T\t" + f[0] + "\t" + f[2] + "\t" + f[3] + "\t" + f[4] + "\t" + f[5] + "\t" + f[6] + "\t" + f[7]);
                }
                ctx.write(key, val);
            } catch (RuntimeException e) {
                ctx.getCounter("Job2", "MALFORMED_LINES").increment(1);
            }
        }
    }

    public static class ReconReducer extends Reducer<Tagged.Key, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private final Map<String, BigDecimal> fx = new HashMap<String, BigDecimal>();      // fx_rates.csv (90 rows) from the Distributed Cache
        private final Text out = new Text();

        protected void setup(Context ctx) throws IOException {
            mos = new MultipleOutputs<NullWritable, Text>(ctx);
            BufferedReader br = new BufferedReader(new FileReader("fx"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",", -1);                       // date,currency,inr_rate
                if (f.length >= 3 && !f[0].equals("date")) fx.put(f[0].trim() + "|" + f[1].trim(), new BigDecimal(f[2].trim()));
            }
            br.close();
            ctx.getCounter("Job2", "FX_RATES_LOADED").setValue(fx.size());
        }

        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }

        private BigDecimal rate(String date, String cur) {
            BigDecimal r = fx.get(date + "|" + cur);
            if (r == null) throw new IllegalStateException("no FX rate for " + cur + " on " + date);
            return r;
        }

        private void emit(String cat, String line) throws IOException, InterruptedException {
            out.set(cat + "\t" + line);
            mos.write("cat", NullWritable.get(), out, cat + "/part");          // one folder per category
        }

        protected void reduce(Tagged.Key key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String orderId = key.id.toString();
            boolean haveOrder = false;
            long orderCents = 0, orderInr = 0;
            String cur = null, orderDate = null;
            BigDecimal rate = null;
            BigDecimal netInrExact = BigDecimal.ZERO;        // running totals: this is ALL that is kept of the many side
            long settled = 0, txns = 0, nOk = 0, nFail = 0, nRef = 0, okUnsettled = 0;

            for (Text v : values) {
                String[] f = v.toString().split("\t");
                if (f[0].equals("O")) {                      // the order arrives FIRST
                    if (haveOrder) { ctx.getCounter("Job2", "DUPLICATE_ORDER_ID").increment(1); continue; }
                    haveOrder = true;
                    orderCents = Long.parseLong(f[1]); cur = f[2]; orderDate = f[3].substring(0, 10);
                    rate = rate(orderDate, cur);                                                       // FX rate of the ORDER date
                    orderInr = Tagged.roundCents(BigDecimal.valueOf(orderCents).multiply(rate));
                    continue;
                }
                // T txn_id amount_cents currency status txn_ts settled_cents settlement_count
                String txnId = f[1]; long amt = Long.parseLong(f[2]); String tcur = f[3], status = f[4], ts = f[5];
                long st = Long.parseLong(f[6]); long sc = Long.parseLong(f[7]);
                if (!haveOrder) {                            // a transaction with NO order in front of it = ORPHAN_TXN
                    ctx.getCounter("Job2", "ORPHAN_TXN").increment(1);
                    BigDecimal r = rate(ts.substring(0, 10), tcur);                                    // no order date: use the transaction date
                    long inr = Tagged.roundCents(BigDecimal.valueOf(status.equals("SUCCESS") ? amt : status.equals("REFUNDED") ? -amt : 0).multiply(r));
                    emit("ORPHAN_TXN", txnId + "\t" + tcur + "\t" + Tagged.format(amt) + "\t0.00\t" + Tagged.format(inr) + "\t" + Tagged.format(st)
                            + "\t" + (status.equals("SUCCESS") ? 1 : 0) + "\t" + (status.equals("FAILED") ? 1 : 0) + "\t" + (status.equals("REFUNDED") ? 1 : 0)
                            + "\t" + (status.equals("SUCCESS") && sc == 0 ? 1 : 0) + "\t0.00\torder_id=" + orderId + " status=" + status + " txn_ts=" + ts);
                    continue;
                }
                txns++;
                settled += st;
                BigDecimal tr = tcur.equals(cur) ? rate : rate(orderDate, tcur);                       // always the rate of the ORDER date
                if (status.equals("SUCCESS")) { nOk++; netInrExact = netInrExact.add(BigDecimal.valueOf(amt).multiply(tr)); if (sc == 0) okUnsettled++; }
                else if (status.equals("REFUNDED")) { nRef++; netInrExact = netInrExact.subtract(BigDecimal.valueOf(amt).multiply(tr)); }
                else nFail++;                                // FAILED: money did not move
            }
            if (!haveOrder) return;

            if (txns > ctx.getCounter("Job2", "MAX_TXNS_FOR_ONE_ORDER").getValue()) ctx.getCounter("Job2", "MAX_TXNS_FOR_ONE_ORDER").setValue(txns);
            if (txns == 0) ctx.getCounter("Job2", "ORDERS_WITHOUT_ANY_TXN").increment(1);
            long netInr = Tagged.roundCents(netInrExact);
            long diff = netInr - orderInr;

            // THE RULES, in the order the question gives them
            String cat;
            if (netInrExact.signum() == 0) cat = "UNPAID";
            else if (Math.abs(diff) > TOLERANCE_PAISE) cat = diff < 0 ? "UNDERPAID" : "OVERPAID";
            else if (okUnsettled > 0) cat = "PAID_NOT_SETTLED";
            else if (Math.abs(settled - netInr) > TOLERANCE_PAISE) cat = "SETTLEMENT_MISMATCH";
            else cat = "MATCHED";
            ctx.getCounter("Job2", cat).increment(1);
            // category, order_id, currency, order_amount, order_amount_inr, net_paid_inr, settled_inr, n_success, n_failed, n_refunded, success_unsettled, net_minus_order_inr, detail
            emit(cat, orderId + "\t" + cur + "\t" + Tagged.format(orderCents) + "\t" + Tagged.format(orderInr) + "\t" + Tagged.format(netInr)
                    + "\t" + Tagged.format(settled) + "\t" + nOk + "\t" + nFail + "\t" + nRef + "\t" + okUnsettled + "\t" + Tagged.format(diff)
                    + "\tfx=" + rate.toPlainString() + " order_date=" + orderDate);
        }
    }

    public int run(String[] args) throws Exception {
        // args: <orders.csv> <job1 output dir (5a/output)>
        // args: <orders.csv> <job1 output dir (5a/output)> <fx_rates.csv> <output dir>
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Q5 job2 orders x txns + classification");
        job.setJarByClass(Reconcile.class);
        job.setMapperClass(ReconMapper.class);
        job.setPartitionerClass(Tagged.Part.class);
        job.setSortComparatorClass(Tagged.SortCmp.class);
        job.setGroupingComparatorClass(Tagged.GroupCmp.class);
        job.setReducerClass(ReconReducer.class);
        job.setMapOutputKeyClass(Tagged.Key.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        MultipleOutputs.addNamedOutput(job, "cat", TextOutputFormat.class, NullWritable.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path j1 = new Path(args[1]);
        FileSystem fs = j1.getFileSystem(conf);
        for (FileStatus s : fs.listStatus(j1))                       // only the part-* files (job 1 output also has an ORPHAN_SETTLEMENT folder)
            if (s.isFile() && s.getPath().getName().startsWith("part-")) FileInputFormat.addInputPath(job, s.getPath());
        Path fxp = new Path(args[2]);
        job.addCacheFile(new URI(fxp.getFileSystem(conf).makeQualified(fxp).toUri().toString() + "#fx"));   // REPLICATED JOIN for the tiny FX table
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Reconcile(), args));
    }
}
