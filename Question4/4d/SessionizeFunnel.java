import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Q4 (a)+(b): sessionize every user's clicks with a SECONDARY SORT.
public class SessionizeFunnel extends Configured implements Tool {

    static final long GAP_MS = 30L * 60 * 1000;               // 30 minutes
    static final long IST_OFFSET_MS = 330L * 60 * 1000;       // IST = UTC + 5:30
    static final DateTimeFormatter ISO_IST = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.of("Asia/Kolkata"));

    static long istDay(long ts) { return Math.floorDiv(ts + IST_OFFSET_MS, 86400000L); }     // day number in IST
    static int stageOf(String url) {
        if (url.equals("/home")) return 1;
        if (url.startsWith("/product/")) return 2;
        if (url.equals("/cart")) return 3;
        if (url.equals("/checkout/success")) return 4;
        return 0;
    }
    static String iso(long ts) { return ISO_IST.format(Instant.ofEpochMilli(ts)); }

    // ---------- 1. THE COMPOSITE KEY: (user_id, event_ts) ----------
    public static class UserTimeKey implements WritableComparable<UserTimeKey> {
        Text user = new Text();
        long ts;
        void set(String u, long t) { user.set(u); ts = t; }
        public void write(DataOutput out) throws IOException { user.write(out); out.writeLong(ts); }
        public void readFields(DataInput in) throws IOException { user.readFields(in); ts = in.readLong(); }
        public int compareTo(UserTimeKey o) {
            int c = user.compareTo(o.user);
            return c != 0 ? c : Long.compare(ts, o.ts);
        }
        public int hashCode() { return user.hashCode(); }
        public boolean equals(Object o) { return o instanceof UserTimeKey && compareTo((UserTimeKey) o) == 0; }
    }

    // ---------- 2. PARTITIONER: looks at user_id ONLY, so one user always goes to one reducer ----------
    public static class UserPartitioner extends Partitioner<UserTimeKey, Text> {
        public int getPartition(UserTimeKey k, Text v, int n) { return (k.user.hashCode() & Integer.MAX_VALUE) % n; }
    }

    // ---------- 3. SORT COMPARATOR: order the keys by user_id, then by time ----------
    public static class SortCmp extends WritableComparator {
        public SortCmp() { super(UserTimeKey.class, true); }
        public int compare(WritableComparable a, WritableComparable b) {
            UserTimeKey x = (UserTimeKey) a, y = (UserTimeKey) b;
            int c = x.user.compareTo(y.user);
            return c != 0 ? c : Long.compare(x.ts, y.ts);
        }
    }

    // ---------- 4. GROUPING COMPARATOR: user_id ONLY, so ONE reduce() call gets ALL events of a user ----------
    public static class GroupCmp extends WritableComparator {
        public GroupCmp() { super(UserTimeKey.class, true); }
        public int compare(WritableComparable a, WritableComparable b) {
            return ((UserTimeKey) a).user.compareTo(((UserTimeKey) b).user);
        }
    }

    // ---------- MAPPER: line "user|ts|url|referrer|agent" -> key (user, ts), value url ----------
    public static class ClickMapper extends Mapper<Object, Text, UserTimeKey, Text> {
        private final UserTimeKey key = new UserTimeKey();
        private final Text val = new Text();
        protected void map(Object off, Text line, Context ctx) throws IOException, InterruptedException {
            ctx.getCounter("Click", "INPUT_LINES").increment(1);
            String[] f = line.toString().split("\\|", -1);
            if (f.length != 5 || f[0].isEmpty() || f[2].isEmpty()) { ctx.getCounter("Click", "MALFORMED_LINES").increment(1); return; }
            long ts;
            try { ts = Long.parseLong(f[1].trim()); }
            catch (NumberFormatException e) { ctx.getCounter("Click", "MALFORMED_LINES").increment(1); return; }
            key.set(f[0], ts);
            val.set(f[2]);
            ctx.write(key, val);
        }
    }

    // ---------- REDUCER: events of ONE user arrive in time order; we read them as a STREAM ----------
    public static class SessionReducer extends Reducer<UserTimeKey, Text, NullWritable, Text> {
        private final Text out = new Text();
        // the state of the CURRENT session only (no list of events anywhere!)
        private String user;
        private int seq;
        private long start, last, events;
        private final Set<String> distinct = new HashSet<String>();
        private String entry, exit;
        private int reachedBefore, reachedNow;     // funnel: stage reached by EARLIER timestamps / stage reached now
        private long batchTs;                      // timestamp of the batch of events we are looking at

        private MultipleOutputs<NullWritable, Text> mos;
        private int minEventsForGapRule;

        protected void setup(Context ctx) {
            mos = new MultipleOutputs<NullWritable, Text>(ctx);
            // 'average gap below 1 second' needs a few events to mean anything (2 clicks in the same millisecond are not a bot)
            minEventsForGapRule = ctx.getConfiguration().getInt("bot.min.events", 5);
        }

        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }

        protected void reduce(UserTimeKey key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            user = key.user.toString();
            ctx.getCounter("Click", "USERS").increment(1);
            seq = 0;
            boolean open = false;
            long userEvents = 0;
            for (Text v : values) {
                long ts = key.ts;                       // the framework changes "key" for every value: this is the secondary-sort trick
                String url = v.toString();
                userEvents++;
                if (open) {
                    boolean byGap = ts - last > GAP_MS;                     // STRICTLY more than 30 min (exactly 30:00 = same session)
                    boolean byDate = istDay(ts) != istDay(last);            // the IST date changed
                    if (ts == last) ctx.getCounter("Click", "EVENTS_WITH_SAME_TIMESTAMP_AS_PREVIOUS").increment(1);
                    if (byGap || byDate) {
                        ctx.getCounter("Click", byGap ? "NEW_SESSION_BY_GAP_OVER_30_MIN" : "NEW_SESSION_BY_IST_DATE_CHANGE").increment(1);
                        closeSession(ctx);
                        open = false;
                    }
                }
                if (!open) {                                                // start a new session
                    seq++; start = ts; last = ts; events = 1;
                    distinct.clear(); distinct.add(url);
                    entry = url; exit = url;
                    reachedBefore = 0; reachedNow = 0; batchTs = ts;
                    stage(url);
                    open = true;
                } else {                                                    // add to the current session
                    events++;
                    distinct.add(url);
                    if (ts == start && url.compareTo(entry) < 0) entry = url;       // same timestamp: fixed tie-break, result never depends on file order
                    if (ts > last) { exit = url; last = ts; }
                    else if (ts == last && url.compareTo(exit) > 0) exit = url;
                    if (ts != batchTs) { reachedBefore = reachedNow; batchTs = ts; }
                    stage(url);
                }
            }
            if (open) closeSession(ctx);
            ctx.getCounter("Click", "EVENTS_PROCESSED").increment(userEvents);
        }

        // a stage counts ONLY if the stage before it was reached at a STRICTLY EARLIER timestamp of the same session
        private void stage(String url) {
            int st = stageOf(url);
            if (st != 0 && st == reachedBefore + 1 && st > reachedNow) reachedNow = st;
        }

        private void closeSession(Context ctx) throws IOException, InterruptedException {
            // user_id, session_seq, start_ts, end_ts, duration_sec, page_views, distinct_urls, entry_url, exit_url
            out.set(user + "\t" + seq + "\t" + iso(start) + "\t" + iso(last) + "\t" + ((last - start) / 1000) + "\t" + events + "\t"
                    + distinct.size() + "\t" + entry + "\t" + exit + "\t" + reachedNow);
            ctx.getCounter("Click", "SESSIONS_TOTAL").increment(1);
            boolean many = events > 500;                                                     // more than 500 events
            boolean fast = events >= minEventsForGapRule && (last - start) < (events - 1) * 1000L;   // average gap < 1 second
            if (many || fast) {
                ctx.getCounter("Click", "SESSIONS_BOT").increment(1);
                if (many) ctx.getCounter("Click", "BOT_MORE_THAN_500_EVENTS").increment(1);
                if (fast) ctx.getCounter("Click", "BOT_AVG_GAP_BELOW_1S").increment(1);
                mos.write("bots", NullWritable.get(), out, "bots");                         // file bots-r-0000x
            } else {
                ctx.getCounter("Click", "SESSIONS_NORMAL").increment(1);
                ctx.write(NullWritable.get(), out);                                          // normal part-r-0000x
            }
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Q4a sessionize");
        job.setJarByClass(SessionizeFunnel.class);
        job.setMapperClass(ClickMapper.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setSortComparatorClass(SortCmp.class);
        job.setGroupingComparatorClass(GroupCmp.class);
        job.setReducerClass(SessionReducer.class);
        job.setMapOutputKeyClass(UserTimeKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        MultipleOutputs.addNamedOutput(job, "bots", TextOutputFormat.class, NullWritable.class, Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SessionizeFunnel(), args));
    }
}
