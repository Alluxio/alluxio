package tachyon.perf.benchmark.simpleread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.benchmark.SimpleTaskContext;

public class SimpleReadTaskContext extends SimpleTaskContext {
  @Override
  public void setFromThread(PerfThread[] threads) {
    mAdditiveStatistics = new HashMap<String, List<Double>>(1);
    List<Double> throughputs = new ArrayList<Double>(threads.length);
    for (PerfThread thread : threads) {
      if (!((SimpleReadThread) thread).getSuccess()) {
        mSuccess = false;
      }
      throughputs.add(((SimpleReadThread) thread).getThroughput());
    }
    mAdditiveStatistics.put("ReadThroughput(MB/s)", throughputs);
  }
}
