# Turns SessionizeFunnel.java (4c) into SessionizeMem.java (4d): same job + two extra counters that PROVE the memory story
s = open("SessionizeFunnel.java").read()

def rep(old, new):
    global s
    assert s.count(old) == 1, "not found exactly once: " + old[:50]
    s = s.replace(old, new)

s = s.replace("SessionizeFunnel", "SessionizeMem")

# every 16384 events: look at how much heap the reducer JVM is using right now, and remember the peak
rep("                userEvents++;\n",
    "                userEvents++;\n"
    "                if ((userEvents & 0x3FFF) == 0) {\n"
    "                    Runtime rt = Runtime.getRuntime();\n"
    "                    long usedMb = (rt.totalMemory() - rt.freeMemory()) >> 20;\n"
    "                    if (usedMb > ctx.getCounter(\"Click\", \"REDUCER_PEAK_HEAP_USED_MB\").getValue())\n"
    "                        ctx.getCounter(\"Click\", \"REDUCER_PEAK_HEAP_USED_MB\").setValue(usedMb);\n"
    "                }\n")

# the biggest group of events that one reduce() call had to handle
rep("            ctx.getCounter(\"Click\", \"EVENTS_PROCESSED\").increment(userEvents);\n",
    "            ctx.getCounter(\"Click\", \"EVENTS_PROCESSED\").increment(userEvents);\n"
    "            if (userEvents > ctx.getCounter(\"Click\", \"MAX_EVENTS_IN_ONE_USER_GROUP\").getValue())\n"
    "                ctx.getCounter(\"Click\", \"MAX_EVENTS_IN_ONE_USER_GROUP\").setValue(userEvents);\n")

open("SessionizeMem.java", "w").write(s)
print("OK: SessionizeMem.java written")
