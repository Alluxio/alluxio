package tachyon.perf.benchmark.skipread;

import java.io.IOException;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.benchmark.SimpleTask;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFileSystem;

public class SkipReadTask extends SimpleTask {
  @Override
  protected boolean setupTask(PerfTaskContext taskContext) {
    try {
      PerfFileSystem fs = PerfFileSystem.get();
      // use the SimpleWrite test data
      String readDir = PerfConf.get().WORK_DIR + "/simple-read-write/" + mId;
      if (!fs.exists(readDir)) {
        throw new IOException("No data to read at " + readDir);
      }
      mTaskConf.addProperty("read.dir", readDir);
      LOG.info("Read dir " + readDir);
    } catch (IOException e) {
      LOG.error("Failed to setup task " + mId, e);
      return false;
    }
    return true;
  }
}
