package tachyon.perf.tools;

import java.io.IOException;

import tachyon.conf.MasterConf;
import tachyon.perf.conf.PerfConf;
import tachyon.perf.fs.PerfFileSystem;

/**
 * A tool to clean the workspace on Tachyon.
 */
public class TachyonPerfCleaner {
  public static void main(String[] args) {
    String tachyonAddress = MasterConf.get().MASTER_ADDRESS;
    try {
      PerfFileSystem fs = PerfFileSystem.get();
      fs.delete(PerfConf.get().WORK_DIR, true);
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Failed to clean workspace " + PerfConf.get().WORK_DIR + " on "
          + tachyonAddress);
    }
    System.out.println("Clean the workspace " + PerfConf.get().WORK_DIR + " on " + tachyonAddress);
  }
}
