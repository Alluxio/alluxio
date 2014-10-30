package tachyon.perf.benchmark.simplewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.benchmark.SimpleTaskContext;

public class SimpleWriteTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(1);
    List<Double> throughputs = new ArrayList<Double>(threads.length);
    for (PerfThread thread : threads) {
      if (!((SimpleWriteThread) thread).getSuccess()) {
        mSuccess = false;
      }
      throughputs.add(((SimpleWriteThread) thread).getThroughput());
    }
    mAdditiveStatistics.put("WriteThroughput(MB/s)", throughputs);
  }
}
