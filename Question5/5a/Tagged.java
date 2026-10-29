import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

// Helper classes shared by both joins of Question 5 (5a and 5b).
public class Tagged {

    // ---------- money is kept as whole cents/paise (no float errors) ----------
    public static long toCents(String s) { return new BigDecimal(s.trim()).setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact(); }
    public static String format(long c) { return (c < 0 ? "-" : "") + Math.abs(c) / 100 + "." + String.format("%02d", Math.abs(c) % 100); }
    public static long roundCents(BigDecimal c) { return c.setScale(0, RoundingMode.HALF_UP).longValueExact(); }

    // ---------- THE TAGGED COMPOSITE KEY: (join id, tag, sub) ----------
    //   tag 0 = the "ONE" side  (sorts FIRST)
    //   tag 1 = the "MANY" side (arrives afterwards, in 'sub' order)
    public static class Key implements WritableComparable<Key> {
        Text id = new Text();
        int tag;
        Text sub = new Text();
        void set(String id, int tag, String sub) { this.id.set(id); this.tag = tag; this.sub.set(sub); }
        public void write(DataOutput out) throws IOException { id.write(out); out.writeInt(tag); sub.write(out); }
        public void readFields(DataInput in) throws IOException { id.readFields(in); tag = in.readInt(); sub.readFields(in); }
        public int compareTo(Key o) {
            int c = id.compareTo(o.id);
            if (c != 0) return c;
            c = Integer.compare(tag, o.tag);
            return c != 0 ? c : sub.compareTo(o.sub);
        }
        public int hashCode() { return id.hashCode(); }
        public boolean equals(Object o) { return o instanceof Key && compareTo((Key) o) == 0; }
    }

    // PARTITIONER: join id only, so the "one" record and all its "many" records reach the same reducer
    public static class Part extends Partitioner<Key, Text> {
        public int getPartition(Key k, Text v, int n) { return (k.id.hashCode() & Integer.MAX_VALUE) % n; }
    }

    // SORT COMPARATOR: (id, tag, sub)  ->  the "one" record comes first, then the "many" records in order
    public static class SortCmp extends WritableComparator {
        public SortCmp() { super(Key.class, true); }
        public int compare(WritableComparable a, WritableComparable b) { return ((Key) a).compareTo((Key) b); }
    }

    // GROUPING COMPARATOR: join id only, so ONE reduce() call per id
    public static class GroupCmp extends WritableComparator {
        public GroupCmp() { super(Key.class, true); }
        public int compare(WritableComparable a, WritableComparable b) { return ((Key) a).id.compareTo(((Key) b).id); }
    }
}
