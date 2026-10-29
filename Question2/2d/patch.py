s = open('Index.java').read()

# 1. imports
s = s.replace("import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;\n",
 "import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;\nimport org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;\nimport org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;\nimport org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;\n")

# 2. reducer: open MultipleOutputs in setup, close in cleanup
s = s.replace("        private long N;\n        protected void setup(Context ctx) throws IOException {\n",
 "        private long N;\n        private MultipleOutputs<Text, Text> mos;\n        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }\n        protected void setup(Context ctx) throws IOException {\n            mos = new MultipleOutputs<Text, Text>(ctx);\n")

# 3. reducer: send each term to the right file
s = s.replace("""            ctx.getCounter("Index", "TERMS").increment(1);
            ctx.write(term, new Text(df + "\\t" + sb));                 // term <TAB> df <TAB> postings
""", """            Text line = new Text(df + "\\t" + sb);                      // df <TAB> postings
            if (df > 0.4 * N) {                                        // in more than 40% of all documents
                ctx.getCounter("Index", "TERMS_CORPUS_STOP").increment(1);
                mos.write("corpusstop", term, line, "corpus_stop");    // file corpus_stop-r-0000x
            } else if (df == 1) {                                      // in exactly one document
                ctx.getCounter("Index", "TERMS_RARE").increment(1);
                mos.write("rare", term, line, "rare");                 // file rare-r-0000x
            } else {
                ctx.getCounter("Index", "TERMS_MAIN_INDEX").increment(1);
                ctx.write(term, line);                                 // normal index: part-r-0000x
            }
""")

# 4. driver: declare the two extra outputs
s = s.replace("        job.setNumReduceTasks(4);\n",
 "        job.setNumReduceTasks(4);\n        MultipleOutputs.addNamedOutput(job, \"corpusstop\", TextOutputFormat.class, Text.class, Text.class);\n        MultipleOutputs.addNamedOutput(job, \"rare\", TextOutputFormat.class, Text.class, Text.class);\n        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);   // no empty part files\n")
open('Index.java', 'w').write(s)
print("patched")
