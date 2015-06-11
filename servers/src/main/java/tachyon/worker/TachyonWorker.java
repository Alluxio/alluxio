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
    BlockWorker worker = null;
    try {
      worker = new BlockWorker(tachyonConf);
      worker.process();
    } catch (Exception e) {
      LOG.error("Uncaught exception, shutting down workers and then exiting.", e);
      try {
        if (worker != null) {
          worker.stop();
        }
      } catch (Exception ex) {
        LOG.error("Failed to stop block worker. Exiting.", e);
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
