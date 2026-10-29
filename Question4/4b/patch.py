# Turns Sessionize.java (4a) into SessionizeBots.java (4b): adds the bot rule + MultipleOutputs
s = open("Sessionize.java").read()

def rep(old, new):
    global s
    assert s.count(old) == 1, "not found exactly once: " + old[:50]
    s = s.replace(old, new)

s = s.replace("Sessionize", "SessionizeBots")

rep("import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\n",
    "import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\n"
    "import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;\n"
    "import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;\n"
    "import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;\n")

rep("        protected void reduce(UserTimeKey key, Iterable<Text> values, Context ctx)",
    "        private MultipleOutputs<NullWritable, Text> mos;\n"
    "        private int minEventsForGapRule;\n\n"
    "        protected void setup(Context ctx) {\n"
    "            mos = new MultipleOutputs<NullWritable, Text>(ctx);\n"
    "            // 'average gap below 1 second' needs a few events to mean anything (2 clicks in the same millisecond are not a bot)\n"
    "            minEventsForGapRule = ctx.getConfiguration().getInt(\"bot.min.events\", 5);\n"
    "        }\n\n"
    "        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }\n\n"
    "        protected void reduce(UserTimeKey key, Iterable<Text> values, Context ctx)")

rep("            ctx.getCounter(\"Click\", \"SESSIONS_TOTAL\").increment(1);\n            ctx.write(NullWritable.get(), out);\n",
    "            ctx.getCounter(\"Click\", \"SESSIONS_TOTAL\").increment(1);\n"
    "            boolean many = events > 500;                                                     // more than 500 events\n"
    "            boolean fast = events >= minEventsForGapRule && (last - start) < (events - 1) * 1000L;   // average gap < 1 second\n"
    "            if (many || fast) {\n"
    "                ctx.getCounter(\"Click\", \"SESSIONS_BOT\").increment(1);\n"
    "                if (many) ctx.getCounter(\"Click\", \"BOT_MORE_THAN_500_EVENTS\").increment(1);\n"
    "                if (fast) ctx.getCounter(\"Click\", \"BOT_AVG_GAP_BELOW_1S\").increment(1);\n"
    "                mos.write(\"bots\", NullWritable.get(), out, \"bots\");                         // file bots-r-0000x\n"
    "            } else {\n"
    "                ctx.getCounter(\"Click\", \"SESSIONS_NORMAL\").increment(1);\n"
    "                ctx.write(NullWritable.get(), out);                                          // normal part-r-0000x\n"
    "            }\n")

rep("        job.setNumReduceTasks(4);\n",
    "        job.setNumReduceTasks(4);\n"
    "        MultipleOutputs.addNamedOutput(job, \"bots\", TextOutputFormat.class, NullWritable.class, Text.class);\n"
    "        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);\n")

open("SessionizeBots.java", "w").write(s)
print("OK: SessionizeBots.java written")
