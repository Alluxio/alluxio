package tachyon.conf;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Unit test for TachyonConf class
 */
public class TachyonConfTest {
  private static final String DEFAULT_HADOOP_UFS_PREFIX = "hdfs://,s3://,s3n://,glusterfs:///";

  private static TachyonConf sDefaultTachyonConf;
  private static Map<String, String> sTestProperties = new LinkedHashMap<String, String>();

  private TachyonConf mCustomPropsTachyonConf;
  private TachyonConf mSystemPropsTachyonConf;

  @AfterClass
  public static void afterClass() {
    System.clearProperty(Constants.MASTER_HOSTNAME);
    System.clearProperty(Constants.MASTER_PORT);
    System.clearProperty(Constants.USE_ZOOKEEPER);
  }

  @BeforeClass
  public static void beforeClass() {
    // initialize the test properties.
    sTestProperties.put("home", "hometest");
    sTestProperties.put("homeandpath", "${home}/path1");
    sTestProperties.put("homeandstring", "${home} string1");
    sTestProperties.put("path2", "path2");
    sTestProperties.put("multiplesubs", "${home}/path1/${path2}");
    sTestProperties.put("recursive", "${multiplesubs}");
    sTestProperties.put("home.port", "8080");
    sTestProperties.put("complex.address", "tachyon://${home}:${home.port}");

    // initialize the system properties
    System.setProperty(Constants.MASTER_HOSTNAME, "master");
    System.setProperty(Constants.MASTER_PORT, "20001");
    System.setProperty(Constants.USE_ZOOKEEPER, "true");

    // initialize
    sDefaultTachyonConf = new TachyonConf(false);
  }

  @Before
  public void beforeTests() {
    // init TachyonConf
    mCustomPropsTachyonConf = new TachyonConf(sTestProperties);
    mSystemPropsTachyonConf = new TachyonConf();
  }

  // test default properties

  @Test
  public void testCommonDefault() {
    String tachyonHome = sDefaultTachyonConf.get(Constants.TACHYON_HOME);
    Assert.assertTrue(tachyonHome != null);
    Assert.assertTrue("/mnt/tachyon_default_home".equals(tachyonHome));

    String ufsAddress = sDefaultTachyonConf.get(Constants.UNDERFS_ADDRESS);
    Assert.assertTrue(ufsAddress != null);
    Assert.assertTrue((tachyonHome + "/underFSStorage").equals(ufsAddress));

    String value = sDefaultTachyonConf.get(Constants.WEB_RESOURCES);
    Assert.assertTrue(value != null);
    Assert.assertTrue((tachyonHome + "/servers/src/main/webapp").equals(value));

    value = sDefaultTachyonConf.get(Constants.UNDERFS_HDFS_IMPL);
    Assert.assertTrue(value != null);
    Assert.assertTrue("org.apache.hadoop.hdfs.DistributedFileSystem".equals(value));

    value = sDefaultTachyonConf.get(Constants.UNDERFS_HADOOP_PREFIXS);
    Assert.assertTrue(value != null);
    Assert.assertTrue(DEFAULT_HADOOP_UFS_PREFIX.equals(value));

    value = sDefaultTachyonConf.get(Constants.UNDERFS_GLUSTERFS_IMPL);
    Assert.assertTrue(value != null);
    Assert.assertTrue("org.apache.hadoop.fs.glusterfs.GlusterFileSystem".equals(value));

    value = sDefaultTachyonConf.get(Constants.UNDERFS_DATA_FOLDER);
    Assert.assertTrue(value != null);
    Assert.assertTrue((ufsAddress + "/tachyon/data").equals(value));

    value = sDefaultTachyonConf.get(Constants.UNDERFS_WORKERS_FOLDER);
    Assert.assertTrue(value != null);
    Assert.assertTrue((ufsAddress + "/tachyon/workers").equals(value));

    boolean booleanValue = sDefaultTachyonConf.getBoolean(Constants.USE_ZOOKEEPER, true);
    Assert.assertTrue(!booleanValue);

    booleanValue = sDefaultTachyonConf.getBoolean(Constants.IN_TEST_MODE, true);
    Assert.assertTrue(!booleanValue);

    booleanValue = sDefaultTachyonConf.getBoolean(Constants.ASYNC_ENABLED, true);
    Assert.assertTrue(!booleanValue);

    int intValue = sDefaultTachyonConf.getInt(Constants.MAX_COLUMNS);
    Assert.assertTrue(intValue == 1000);

    intValue = sDefaultTachyonConf.getInt(Constants.HOST_RESOLUTION_TIMEOUT_MS);
    Assert.assertEquals(Constants.DEFAULT_HOST_RESOLUTION_TIMEOUT_MS, intValue);

    long longBytesValue = sDefaultTachyonConf.getBytes(Constants.MAX_TABLE_METADATA_BYTE);
    Assert.assertTrue(longBytesValue == Constants.MB * 5);
  }

  @Test
  public void testMasterDefault() {
    String tachyonHome = sDefaultTachyonConf.get(Constants.TACHYON_HOME);
    Assert.assertTrue(tachyonHome != null);
    Assert.assertTrue("/mnt/tachyon_default_home".equals(tachyonHome));

    String value = sDefaultTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
    Assert.assertTrue(value != null);
    Assert.assertTrue((tachyonHome + "/journal/").equals(value));

    value = sDefaultTachyonConf.get(Constants.MASTER_HOSTNAME);
    Assert.assertTrue(value != null);
    Assert.assertTrue(NetworkAddressUtils.getLocalHostName(100).equals(value));

    value = sDefaultTachyonConf.get(Constants.MASTER_TEMPORARY_FOLDER);
    Assert.assertTrue(value != null);
    Assert.assertTrue("/tmp".equals(value));

    value = sDefaultTachyonConf.get(Constants.MASTER_FORMAT_FILE_PREFIX);
    Assert.assertTrue(value != null);
    Assert.assertTrue(Constants.FORMAT_FILE_PREFIX.equals(value));

    value = sDefaultTachyonConf.get(Constants.MASTER_ADDRESS);
    Assert.assertTrue(value != null);

    int intValue = sDefaultTachyonConf.getInt(Constants.MASTER_PORT);
    Assert.assertTrue(intValue == 19998);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_WEB_PORT);
    Assert.assertTrue(intValue == 19999);

    intValue = sDefaultTachyonConf.getInt(Constants.WEB_THREAD_COUNT);
    Assert.assertTrue(intValue == 1);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS);
    Assert.assertTrue(intValue == Constants.SECOND_MS);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_MIN_WORKER_THREADS);
    Assert.assertTrue(intValue == Runtime.getRuntime().availableProcessors());

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_WORKER_TIMEOUT_MS);
    Assert.assertTrue(intValue == 10 * Constants.SECOND_MS);
  }

  @Test
  public void testWorkerDefault() {
    String value = sDefaultTachyonConf.get(Constants.WORKER_DATA_FOLDER);
    Assert.assertTrue(value != null);
    Assert.assertTrue(("/mnt/ramdisk").equals(value));

    int intValue = sDefaultTachyonConf.getInt(Constants.WORKER_PORT);
    Assert.assertTrue(intValue == 29998);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_DATA_PORT);
    Assert.assertTrue(intValue == 29999);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS);
    Assert.assertTrue(intValue == 10 * Constants.SECOND_MS);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    Assert.assertTrue(intValue == Constants.SECOND_MS);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_MIN_WORKER_THREADS);
    Assert.assertTrue(intValue == Runtime.getRuntime().availableProcessors());

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_USER_TIMEOUT_MS);
    Assert.assertTrue(intValue == 10 * Constants.SECOND_MS);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_CHECKPOINT_THREADS);
    Assert.assertTrue(intValue == 1);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC);
    Assert.assertTrue(intValue == 1000);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_NETTY_BOSS_THREADS);
    Assert.assertTrue(intValue == 1);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_NETTY_WORKER_THREADS);
    Assert.assertTrue(intValue == 0);

    long longValue = sDefaultTachyonConf.getBytes(Constants.WORKER_MEMORY_SIZE);
    Assert.assertTrue(longValue == (128 * Constants.MB));
  }

  @Test
  public void testUserDefault() {
    int intValue = sDefaultTachyonConf.getInt(Constants.USER_FAILED_SPACE_REQUEST_LIMITS);
    Assert.assertTrue(intValue == 3);

    intValue = sDefaultTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS);
    Assert.assertTrue(intValue == Constants.SECOND_MS);

    long longValue = sDefaultTachyonConf.getBytes(Constants.USER_QUOTA_UNIT_BYTES);
    Assert.assertTrue(longValue == (8 * Constants.MB));

    longValue = sDefaultTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES);
    Assert.assertTrue(longValue == Constants.MB);

    longValue = sDefaultTachyonConf.getBytes(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE);
    Assert.assertTrue(longValue == 8 * Constants.MB);
  }

  @Test
  public void testVariableSubstitutionSimple() {
    String home = mCustomPropsTachyonConf.get("home");
    Assert.assertTrue("hometest".equals(home));

    String homeAndPath = mCustomPropsTachyonConf.get("homeandpath");
    Assert.assertTrue((home + "/path1").equals(homeAndPath));

    String homeAndString = mCustomPropsTachyonConf.get("homeandstring");
    Assert.assertTrue((home + " string1").equals(homeAndString));

    String path2 = mCustomPropsTachyonConf.get("path2");
    Assert.assertTrue("path2".equals(path2));

    String multiplesubs = mCustomPropsTachyonConf.get("multiplesubs");
    Assert.assertTrue((home + "/path1/" + path2).equals(multiplesubs));

    String homePort = mCustomPropsTachyonConf.get("home.port");
    Assert.assertTrue(("8080").equals(homePort));

    sTestProperties.put("complex.address", "tachyon://${home}:${home.port}");
    String complexAddress = mCustomPropsTachyonConf.get("complex.address");
    Assert.assertTrue(("tachyon://" + home + ":" + homePort).equals(complexAddress));

  }

  @Test
  public void testVariableSubstitutionRecursive() {
    String multiplesubs = mCustomPropsTachyonConf.get("multiplesubs");
    String recursive = mCustomPropsTachyonConf.get("recursive");
    Assert.assertTrue(multiplesubs.equals(recursive));
  }

  @Test
  public void testSystemVariableSubstitutionSample() {
    String masterAddress = mSystemPropsTachyonConf.get(Constants.MASTER_ADDRESS);
    Assert.assertTrue(masterAddress != null);
    Assert.assertTrue("tachyon-ft://master:20001".equals(masterAddress));
  }
}
