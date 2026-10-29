import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

// ONE posting = one document that contains a term: doc_id, tf, and the positions.
// Positions are stored as GAPS with writeVInt:  [3, 10, 25]  is stored as  3, 7, 15
public class PostingWritable implements Writable {
    public static boolean DELTA = true;          // switch, so we can test with and without gap encoding

    public String docId = "";
    public int tf;
    public int count;                            // how many positions
    public int[] positions = new int[0];

    public void set(String docId, int tf, int[] positions, int count) {
        this.docId = docId; this.tf = tf; this.positions = positions; this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, docId);
        WritableUtils.writeVInt(out, tf);
        WritableUtils.writeVInt(out, count);
        int prev = 0;
        for (int i = 0; i < count; i++) {
            WritableUtils.writeVInt(out, DELTA ? positions[i] - prev : positions[i]);
            prev = positions[i];
        }
    }

    public void readFields(DataInput in) throws IOException {
        docId = WritableUtils.readString(in);
        tf = WritableUtils.readVInt(in);
        count = WritableUtils.readVInt(in);
        if (positions.length < count) positions = new int[count];
        int prev = 0;
        for (int i = 0; i < count; i++) {
            int v = WritableUtils.readVInt(in);
            prev = DELTA ? prev + v : v;         // turn gaps back into real positions
            positions[i] = prev;
        }
    }
}
