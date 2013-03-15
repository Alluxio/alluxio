package tachyon;

import org.apache.log4j.Logger;

/**
 * Format Tachyon.
 */
public class Format {
  private final static Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);
  public static void main(String[] args) {
    if (args.length == 0) {
      LOG.info("Deleting " + Config.MASTER_CHECKPOINT_FILE);
      CommonUtils.deleteFile(Config.MASTER_CHECKPOINT_FILE);
      LOG.info("Deleting " + Config.MASTER_LOG_FILE);
      CommonUtils.deleteFile(Config.MASTER_LOG_FILE);

      if (Config.USING_HDFS) {
        HdfsClient hdfsClient = new HdfsClient(Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER);
        LOG.info("Deleting " + Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER);
        hdfsClient.delete(Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER, true);
        LOG.info("Deleting " + Config.HDFS_ADDRESS + Config.WORKER_HDFS_FOLDER);
        hdfsClient.delete(Config.HDFS_ADDRESS + Config.WORKER_HDFS_FOLDER, true);
      }
    } else {
      LOG.info("java -cp target/tachyon-" + Version.VERSION +
          "-jar-with-dependencies.jar tachyon.Format");
    }
  }
}