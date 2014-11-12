package tachyon.perf.benchmark.foo;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.PerfTaskContext;

public class FooTask extends PerfTask {
  @Override
  public String getCleanupDir() {
    return "Foo";
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    this.mTaskConf.addProperty("foo", "true");
    return true;
  }

  @Override
  protected boolean cleanupTask(PerfTaskContext taskContext) {
    ((FooTaskContext) taskContext).setFoo(5);
    return true;
  }
}
