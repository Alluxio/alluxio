package tachyon.perf.benchmark.metadata;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.benchmark.SimpleTask;
import tachyon.perf.conf.PerfConf;

public class MetadataTask extends SimpleTask {
  @Override
  public String getCleanupDir() {
    return PerfConf.get().WORK_DIR + "/metadata";
  }

  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String workDir = PerfConf.get().WORK_DIR + "/metadata/" + mId;
    mTaskConf.addProperty("work.dir", workDir);
    LOG.info("Work dir " + workDir);
    return true;
  }
}
