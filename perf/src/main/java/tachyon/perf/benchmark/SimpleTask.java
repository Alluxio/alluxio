package tachyon.perf.benchmark;

import tachyon.perf.basic.PerfTask;
import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.conf.PerfConf;

/**
 * An simple example to extend PerfTask. It can be used to quickly implement a new benchmark.
 */
public class SimpleTask extends PerfTask {
  @Override
  protected boolean cleanupTask(PerfTaskContext taskContext) {
    return true;
  }

  @Override
  public String getCleanupDir() {
    return null;
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workspacePath = PerfConf.get().WORK_DIR;
    LOG.info("Perf workspace " + workspacePath);
    return true;
  }
}
