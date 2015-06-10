package tachyon.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockWorker;

/**
 * Entry point for the Tachyon Worker. This class is responsible for initializing the different
 * workers that are configured to run.
 */
public class TachyonWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static void main(String[] args) {
    checkArgs(args);
    TachyonConf tachyonConf = new TachyonConf();
    BlockWorker worker = new BlockWorker(tachyonConf);
    try {
      worker.process();
    } catch (Exception e) {
      LOG.error("Uncaught exception, shutting down Tachyon Worker", e);
      try {
        worker.stop();
      } catch (Exception ex) {
        LOG.error("Failed to stop block worker.", e);
        System.exit(-1);
      }
      System.exit(-1);
    }
    System.exit(0);
  }

  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java TachyonWorker");
      System.exit(-1);
    }
  }
}
