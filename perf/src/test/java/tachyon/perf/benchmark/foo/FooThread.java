package tachyon.perf.benchmark.foo;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.basic.TaskConfiguration;

public class FooThread extends PerfThread {
  private boolean mCleanup = false;
  private boolean mSetup = false;
  private boolean mRun = false;

  @Override
  public void run() {
    mRun = true;
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mSetup = taskConf.getBooleanProperty("foo");
    return true;
  }

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    mCleanup = true;
    return true;
  }

  public boolean getCleanup() {
    return mCleanup;
  }

  public boolean getSetup() {
    return mSetup;
  }

  public boolean getRun() {
    return mRun;
  }
}
