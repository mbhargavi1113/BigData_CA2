# Turns SessionizeBots.java (4b) into SessionizeFunnel.java (4c): adds the "funnel stage reached" as a 10th column
s = open("SessionizeBots.java").read()

def rep(old, new):
    global s
    assert s.count(old) == 1, "not found exactly once: " + old[:50]
    s = s.replace(old, new)

s = s.replace("SessionizeBots", "SessionizeFunnel")

# 1. which stage is a url?   /home=1  /product/*=2  /cart=3  /checkout/success=4
rep("    static String iso(long ts) {",
    "    static int stageOf(String url) {\n"
    "        if (url.equals(\"/home\")) return 1;\n"
    "        if (url.startsWith(\"/product/\")) return 2;\n"
    "        if (url.equals(\"/cart\")) return 3;\n"
    "        if (url.equals(\"/checkout/success\")) return 4;\n"
    "        return 0;\n"
    "    }\n"
    "    static String iso(long ts) {")

# 2. two more numbers in the state of the current session
rep("        private String entry, exit;\n",
    "        private String entry, exit;\n"
    "        private int reachedBefore, reachedNow;     // funnel: stage reached by EARLIER timestamps / stage reached now\n"
    "        private long batchTs;                      // timestamp of the batch of events we are looking at\n")

# 3. first event of a session
rep("                    entry = url; exit = url;\n                    open = true;\n",
    "                    entry = url; exit = url;\n"
    "                    reachedBefore = 0; reachedNow = 0; batchTs = ts;\n"
    "                    stage(url);\n"
    "                    open = true;\n")

# 4. every next event of the session
rep("                    else if (ts == last && url.compareTo(exit) > 0) exit = url;\n",
    "                    else if (ts == last && url.compareTo(exit) > 0) exit = url;\n"
    "                    if (ts != batchTs) { reachedBefore = reachedNow; batchTs = ts; }\n"
    "                    stage(url);\n")

# 5. the method that moves the funnel forward
rep("        private void closeSession(Context ctx)",
    "        // a stage counts ONLY if the stage before it was reached at a STRICTLY EARLIER timestamp of the same session\n"
    "        private void stage(String url) {\n"
    "            int st = stageOf(url);\n"
    "            if (st != 0 && st == reachedBefore + 1 && st > reachedNow) reachedNow = st;\n"
    "        }\n\n"
    "        private void closeSession(Context ctx)")

# 6. write the stage as the 10th column
rep("+ distinct.size() + \"\\t\" + entry + \"\\t\" + exit);",
    "+ distinct.size() + \"\\t\" + entry + \"\\t\" + exit + \"\\t\" + reachedNow);")

open("SessionizeFunnel.java", "w").write(s)
print("OK: SessionizeFunnel.java written")
