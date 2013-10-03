package tachyon;

import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;

/**
 * Format Tachyon File System.
 */
public class Format {
  private final static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public static void main(String[] args) throws IOException {
    if (args.length != 0) {
      LOG.info("java -cp tachyon-server/target/tachyon-server-" + Version.VERSION +
          "-jar-with-dependencies.jar tachyon.Format");
      System.exit(-1);
    }

    MasterConf masterConf = MasterConf.get();
    UnderFileSystem ufs = UnderFileSystem.get(masterConf.JOURNAL_FOLDER);
    LOG.info("Deleting " + masterConf.JOURNAL_FOLDER);
    if (!ufs.delete(masterConf.JOURNAL_FOLDER, true)) {
      LOG.error("Failed to remove " + masterConf.JOURNAL_FOLDER);
    }
    ufs.mkdirs(masterConf.JOURNAL_FOLDER, true);

    CommonConf commonConf = CommonConf.get();
    String folder = commonConf.UNDERFS_DATA_FOLDER;
    ufs = UnderFileSystem.get(folder);
    LOG.info("Formatting " + folder);
    ufs.delete(folder, true);
    if (!ufs.mkdirs(folder, true)) {
      LOG.info("Failed to create " + folder);
    }

    folder = commonConf.UNDERFS_WORKERS_FOLDER;
    LOG.info("Formatting " + folder);
    ufs.delete(folder, true);
    if (!ufs.mkdirs(folder, true)) {
      LOG.info("Failed to create " + folder);
    }
  }
}
