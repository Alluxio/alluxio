package tachyon.perf.benchmark.simplewrite;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.benchmark.SimpleTask;
import tachyon.perf.conf.PerfConf;

public class SimpleWriteTask extends SimpleTask {
  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    String writeDir = PerfConf.get().WORK_DIR + "/simple-read-write/" + mId;
    mTaskConf.addProperty("write.dir", writeDir);
    LOG.info("Write dir " + writeDir);
    return true;
  }
}
