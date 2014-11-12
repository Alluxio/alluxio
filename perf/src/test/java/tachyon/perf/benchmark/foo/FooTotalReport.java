package tachyon.perf.benchmark.foo;

import java.io.File;
import java.io.IOException;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.basic.PerfTotalReport;

public class FooTotalReport extends PerfTotalReport {
  private int mFoo = 0;
  private boolean mWrite = false;

  @Override
  public void initialFromTaskContexts(PerfTaskContext[] taskContexts) throws IOException {
    for (PerfTaskContext taskContext : taskContexts) {
      mFoo += ((FooTaskContext) taskContext).getFoo();
    }
  }

  @Override
  public void writeToFile(File file) throws IOException {
    mWrite = true;
  }

  public int getFoo() {
    return mFoo;
  }

  public boolean getWrite() {
    return mWrite;
  }
}
