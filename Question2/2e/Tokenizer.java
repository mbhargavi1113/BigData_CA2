import java.util.List;
import java.util.Locale;
import java.util.Set;

// The tokenising rules. We will COPY this file into 2c, 2d and 2e later.
public class Tokenizer {
    // adds the kept tokens of 'text' to 'out'.
    // dropped[0] counts too-short tokens, dropped[1] counts stop-words.
    public static void tokenize(String text, Set<String> stop, List<String> out, long[] dropped) {
        String s = text.toLowerCase(Locale.ROOT);
        int n = s.length(), i = 0;
        while (i < n) {
            while (i < n && !Character.isLetterOrDigit(s.charAt(i))) i++;   // skip separators
            int start = i;
            while (i < n && Character.isLetterOrDigit(s.charAt(i))) i++;    // read one word
            if (i > start) {
                String t = s.substring(start, i);
                if (t.length() < 2) dropped[0]++;
                else if (stop.contains(t)) dropped[1]++;
                else out.add(t);
            }
        }
    }
}
