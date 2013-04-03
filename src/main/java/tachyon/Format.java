package tachyon;

import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;

/**
 * Format Tachyon.
 */
public class Format {
  private final static Logger LOG = Logger.getLogger(CommonConf.get().LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp target/tachyon-" + Version.VERSION +
          "-jar-with-dependencies.jar tachyon.Format");
      System.exit(-1);
    }

    MasterConf masterConf = MasterConf.get();
    LOG.info("Deleting " + masterConf.CHECKPOINT_FILE);
    CommonUtils.deleteFile(masterConf.CHECKPOINT_FILE);
    LOG.info("Deleting " + masterConf.LOG_FILE);
    CommonUtils.deleteFile(masterConf.LOG_FILE);

    CommonConf commonConf = CommonConf.get();
    if (commonConf.USING_HDFS) {
      String folder = commonConf.HDFS_ADDRESS + commonConf.DATA_FOLDER;
      HdfsClient hdfsClient = new HdfsClient(folder);
      LOG.info("Deleting " + folder);
      hdfsClient.delete(folder, true);
      hdfsClient.mkdirs(folder, null, true);

      folder = commonConf.HDFS_ADDRESS + commonConf.WORKERS_FOLDER;
      LOG.info("Deleting " + folder);
      hdfsClient.delete(folder, true);
      hdfsClient.mkdirs(folder, null, true);
    }
  }
}