package tachyon.conf;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import tachyon.Constants;

/**
 * Configurations shared by master and workers.
 */
public class CommonConf extends Utils {
  private static final Logger LOG = Logger.getLogger("");

  private static CommonConf COMMON_CONF = null;

  public static final ImmutableList<String> DEFAULT_HADOOP_UFS_PREFIX = ImmutableList.of(
      "hdfs://", "s3://", "s3n://", "glusterfs:///", "ceph://");

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    COMMON_CONF = null;
  }

  public static synchronized CommonConf get() {
    if (COMMON_CONF == null) {
      COMMON_CONF = new CommonConf();
    }

    return COMMON_CONF;
  }

  public final String TACHYON_HOME;
  public final String UNDERFS_ADDRESS;
  public final String UNDERFS_DATA_FOLDER;
  public final String UNDERFS_WORKERS_FOLDER;

  public final String UNDERFS_HDFS_IMPL;
  public final String UNDERFS_GLUSTERFS_IMPL;
  public final String UNDERFS_GLUSTERFS_VOLUMES;
  public final String UNDERFS_GLUSTERFS_MOUNTS;
  public final String UNDERFS_GLUSTERFS_MR_DIR;
  public final String WEB_RESOURCES;
  public final boolean USE_ZOOKEEPER;
  public final String ZOOKEEPER_ADDRESS;

  public final String ZOOKEEPER_ELECTION_PATH;

  public final String ZOOKEEPER_LEADER_PATH;

  public final boolean ASYNC_ENABLED;

  public final int MAX_COLUMNS;

  public final int MAX_TABLE_METADATA_BYTE;

  public final ImmutableList<String> HADOOP_UFS_PREFIXES;
  
  public final boolean IN_TEST_MODE;

  private CommonConf() {
    if (System.getProperty("tachyon.home") == null) {
      LOG.warn("tachyon.home is not set. Using /mnt/tachyon_default_home as the default value.");
      File file = new File("/mnt/tachyon_default_home");
      if (!file.exists()) {
        file.mkdirs();
      }
    }
    TACHYON_HOME = getProperty("tachyon.home", "/mnt/tachyon_default_home");
    WEB_RESOURCES = getProperty("tachyon.web.resources", TACHYON_HOME + "/core/src/main/webapp");
    UNDERFS_ADDRESS = getProperty("tachyon.underfs.address", TACHYON_HOME + "/underfs");
    UNDERFS_DATA_FOLDER = getProperty("tachyon.data.folder", UNDERFS_ADDRESS + "/tachyon/data");
    UNDERFS_WORKERS_FOLDER =
        getProperty("tachyon.workers.folder", UNDERFS_ADDRESS + "/tachyon/workers");
    UNDERFS_HDFS_IMPL =
        getProperty("tachyon.underfs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    UNDERFS_GLUSTERFS_IMPL =
        getProperty("tachyon.underfs.glusterfs.impl",
            "org.apache.hadoop.fs.glusterfs.GlusterFileSystem");
    UNDERFS_GLUSTERFS_VOLUMES = getProperty("tachyon.underfs.glusterfs.volumes", null);
    UNDERFS_GLUSTERFS_MOUNTS = getProperty("tachyon.underfs.glusterfs.mounts", null);
    UNDERFS_GLUSTERFS_MR_DIR =
        getProperty("tachyon.underfs.glusterfs.mapred.system.dir", "glusterfs:///mapred/system");
    USE_ZOOKEEPER = getBooleanProperty("tachyon.usezookeeper", false);
    if (USE_ZOOKEEPER) {
      ZOOKEEPER_ADDRESS = getProperty("tachyon.zookeeper.address");
      ZOOKEEPER_ELECTION_PATH = getProperty("tachyon.zookeeper.election.path", "/election");
      ZOOKEEPER_LEADER_PATH = getProperty("tachyon.zookeeper.leader.path", "/leader");
    } else {
      ZOOKEEPER_ADDRESS = null;
      ZOOKEEPER_ELECTION_PATH = null;
      ZOOKEEPER_LEADER_PATH = null;
    }

    ASYNC_ENABLED = getBooleanProperty("tachyon.async.enabled", false);

    MAX_COLUMNS = getIntProperty("tachyon.max.columns", 1000);
    MAX_TABLE_METADATA_BYTE = getIntProperty("tachyon.max.table.metadata.byte", Constants.MB * 5);

    HADOOP_UFS_PREFIXES =
        getListProperty("tachyon.underfs.hadoop.prefixes", DEFAULT_HADOOP_UFS_PREFIX);

    IN_TEST_MODE = getBooleanProperty("tachyon.test.mode", false);
  }

  public static void assertValidPort(final int port) {
    if (!get().IN_TEST_MODE) {
      Preconditions.checkArgument(port > 0, "Port is only allowed to be zero in test mode.");
    }
  }

  public static void assertValidPort(final InetSocketAddress address) {
    assertValidPort(address.getPort());
  }
}
