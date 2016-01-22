package tachyon.conf;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Unit test for TachyonConf class
 */
public class TachyonConfTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();
  private static final String DEFAULT_HADOOP_UFS_PREFIX = "hdfs://,glusterfs:///";

  private static TachyonConf sDefaultTachyonConf;
  private static Map<String, String> sTestProperties = new LinkedHashMap<String, String>();

  private TachyonConf mCustomPropsTachyonConf;
  private TachyonConf mSystemPropsTachyonConf;

  /**
   * Clears the properties after the test suite is finished.
   */
  @AfterClass
  public static void afterClass() {
    System.clearProperty(Constants.MASTER_HOSTNAME);
    System.clearProperty(Constants.MASTER_RPC_PORT);
    System.clearProperty(Constants.ZOOKEEPER_ENABLED);
  }

  /**
   * Sets the properties and configuration before the test suite runs.
   */
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
    System.setProperty(Constants.MASTER_RPC_PORT, "20001");
    System.setProperty(Constants.ZOOKEEPER_ENABLED, "true");

    // initialize
    sDefaultTachyonConf = new TachyonConf(false);
  }

  /**
   * Creates new configurations before a test runs.
   */
  @Before
  public void beforeTests() {
    // init TachyonConf
    mCustomPropsTachyonConf = new TachyonConf(sTestProperties);
    mSystemPropsTachyonConf = new TachyonConf();
  }

  /**
   * Tests the default common properties.
   */
  @Test
  public void commonDefaultTest() {
    String tachyonHome = sDefaultTachyonConf.get(Constants.TACHYON_HOME);
    Assert.assertNotNull(tachyonHome);
    Assert.assertEquals("/mnt/tachyon_default_home", tachyonHome);

    String ufsAddress = sDefaultTachyonConf.get(Constants.UNDERFS_ADDRESS);
    Assert.assertNotNull(ufsAddress);
    Assert.assertEquals(tachyonHome + "/underFSStorage", ufsAddress);

    String value = sDefaultTachyonConf.get(Constants.WEB_RESOURCES);
    Assert.assertNotNull(value);
    Assert.assertEquals(tachyonHome + "/servers/src/main/webapp", value);

    value = sDefaultTachyonConf.get(Constants.UNDERFS_HDFS_IMPL);
    Assert.assertNotNull(value);
    Assert.assertEquals("org.apache.hadoop.hdfs.DistributedFileSystem", value);

    value = sDefaultTachyonConf.get(Constants.UNDERFS_HDFS_PREFIXS);
    Assert.assertNotNull(value);
    Assert.assertEquals(DEFAULT_HADOOP_UFS_PREFIX, value);

    value = sDefaultTachyonConf.get(Constants.UNDERFS_GLUSTERFS_IMPL);
    Assert.assertNotNull(value);
    Assert.assertEquals("org.apache.hadoop.fs.glusterfs.GlusterFileSystem", value);

    boolean booleanValue = sDefaultTachyonConf.getBoolean(Constants.ZOOKEEPER_ENABLED);
    Assert.assertFalse(booleanValue);

    booleanValue = sDefaultTachyonConf.getBoolean(Constants.IN_TEST_MODE);
    Assert.assertFalse(booleanValue);

    int intValue = sDefaultTachyonConf.getInt(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    Assert.assertEquals(Constants.DEFAULT_HOST_RESOLUTION_TIMEOUT_MS, intValue);

    long longBytesValue =
        sDefaultTachyonConf.getBytes(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
    Assert.assertEquals(Constants.MB * 8, longBytesValue);

    int maxTry = sDefaultTachyonConf.getInt(Constants.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT);
    Assert.assertEquals(10, maxTry);
  }

  /**
   * Tests the default properties for the master.
   */
  @Test
  public void masterDefaultTest() {
    String tachyonHome = sDefaultTachyonConf.get(Constants.TACHYON_HOME);
    Assert.assertNotNull(tachyonHome);
    Assert.assertEquals("/mnt/tachyon_default_home", tachyonHome);

    String value = sDefaultTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
    Assert.assertNotNull(value);
    Assert.assertEquals(tachyonHome + "/journal/", value);

    value = sDefaultTachyonConf.get(Constants.MASTER_HOSTNAME);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.getLocalHostName(100), value);

    value = sDefaultTachyonConf.get(Constants.MASTER_FORMAT_FILE_PREFIX);
    Assert.assertNotNull(value);
    Assert.assertEquals(Constants.FORMAT_FILE_PREFIX, value);

    value = sDefaultTachyonConf.get(Constants.MASTER_ADDRESS);
    Assert.assertNotNull(value);

    value = sDefaultTachyonConf.get(Constants.MASTER_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    int intValue = sDefaultTachyonConf.getInt(Constants.MASTER_RPC_PORT);
    Assert.assertEquals(19998, intValue);

    value = sDefaultTachyonConf.get(Constants.MASTER_WEB_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_WEB_PORT);
    Assert.assertEquals(19999, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WEB_THREAD_COUNT);
    Assert.assertEquals(1, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(Constants.SECOND_MS, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_WORKER_THREADS_MIN);
    Assert.assertEquals(Runtime.getRuntime().availableProcessors(), intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.MASTER_WORKER_TIMEOUT_MS);
    Assert.assertEquals(10 * Constants.SECOND_MS, intValue);
  }

  /**
   * Tests the default properties for the worker.
   */
  @Test
  public void workerDefaultTest() {
    String value = sDefaultTachyonConf.get(Constants.WORKER_DATA_FOLDER);
    Assert.assertNotNull(value);
    Assert.assertEquals("/tachyonworker/", value);

    value = sDefaultTachyonConf.get(Constants.WORKER_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    int intValue = sDefaultTachyonConf.getInt(Constants.WORKER_RPC_PORT);
    Assert.assertEquals(29998, intValue);

    value = sDefaultTachyonConf.get(Constants.WORKER_DATA_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_DATA_PORT);
    Assert.assertEquals(29999, intValue);

    value = sDefaultTachyonConf.get(Constants.WORKER_WEB_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_WEB_PORT);
    Assert.assertEquals(30000, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);
    Assert.assertEquals(10 * Constants.SECOND_MS, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(Constants.SECOND_MS, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MIN);
    Assert.assertEquals(1, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_SESSION_TIMEOUT_MS);
    Assert.assertEquals(10 * Constants.SECOND_MS, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_BOSS_THREADS);
    Assert.assertEquals(1, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS);
    Assert.assertEquals(0, intValue);

    long longValue = sDefaultTachyonConf.getBytes(Constants.WORKER_MEMORY_SIZE);
    Assert.assertEquals(128 * Constants.MB, longValue);
  }

  /**
   * Tests the default properties for the user.
   */
  @Test
  public void userDefaultTest() {
    int intValue = sDefaultTachyonConf.getInt(Constants.USER_FAILED_SPACE_REQUEST_LIMITS);
    Assert.assertEquals(3, intValue);

    intValue = sDefaultTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(Constants.SECOND_MS, intValue);

    long longValue = sDefaultTachyonConf.getBytes(Constants.USER_QUOTA_UNIT_BYTES);
    Assert.assertEquals(8 * Constants.MB, longValue);

    longValue = sDefaultTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES);
    Assert.assertEquals(Constants.MB, longValue);

    longValue = sDefaultTachyonConf.getBytes(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
    Assert.assertEquals(8 * Constants.MB, longValue);
  }

  /**
   * Tests the simple substitution of variables.
   */
  @Test
  public void variableSubstitutionSimpleTest() {
    String home = mCustomPropsTachyonConf.get("home");
    Assert.assertEquals("hometest", home);

    String homeAndPath = mCustomPropsTachyonConf.get("homeandpath");
    Assert.assertEquals(home + "/path1", homeAndPath);

    String homeAndString = mCustomPropsTachyonConf.get("homeandstring");
    Assert.assertEquals(home + " string1", homeAndString);

    String path2 = mCustomPropsTachyonConf.get("path2");
    Assert.assertEquals("path2", path2);

    String multiplesubs = mCustomPropsTachyonConf.get("multiplesubs");
    Assert.assertEquals(home + "/path1/" + path2, multiplesubs);

    String homePort = mCustomPropsTachyonConf.get("home.port");
    Assert.assertEquals("8080", homePort);

    sTestProperties.put("complex.address", "tachyon://${home}:${home.port}");
    String complexAddress = mCustomPropsTachyonConf.get("complex.address");
    Assert.assertEquals("tachyon://" + home + ":" + homePort, complexAddress);

  }

  /**
   * Tests the recursive substitution of variables.
   */
  @Test
  public void variableSubstitutionRecursiveTest() {
    String multiplesubs = mCustomPropsTachyonConf.get("multiplesubs");
    String recursive = mCustomPropsTachyonConf.get("recursive");
    Assert.assertEquals(multiplesubs, recursive);
  }

  /**
   * Tests the substitution of system variables.
   */
  @Test
  public void systemVariableSubstitutionSampleTest() {
    String masterAddress = mSystemPropsTachyonConf.get(Constants.MASTER_ADDRESS);
    Assert.assertNotNull(masterAddress);
    Assert.assertEquals("tachyon-ft://master:20001", masterAddress);
  }

  /**
   * Tests that an exception is thrown when the {@link Constants#USER_FILE_BUFFER_BYTES} overflows.
   */
  @Test
  public void variableUserFileBufferBytesOverFlowCheckTest() {
    Properties mProperties = new Properties();
    mProperties.put(Constants.USER_FILE_BUFFER_BYTES ,
            String.valueOf(Integer.MAX_VALUE + 1) + "B");
    mThrown.expect(IllegalArgumentException.class);
    new TachyonConf(mProperties);
  }

  /**
   * Tests that an exception is thrown when the {@link Constants#USER_FILE_BUFFER_BYTES} overflows.
   */
  @Test
  public void variableUserFileBufferBytesOverFlowCheckTest1() {
    Map<String, String> properties = new LinkedHashMap<String, String>();
    properties.put(Constants.USER_FILE_BUFFER_BYTES ,
            String.valueOf(Integer.MAX_VALUE + 1) + "B");
    mThrown.expect(IllegalArgumentException.class);
    new TachyonConf(properties);
  }

  /**
   * Tests that setting the {@link Constants#USER_FILE_BUFFER_BYTES} runs correctly.
   */
  @Test
  public void variableUserFileBufferBytesNormalCheckTest() {
    Properties mProperties = new Properties();
    mProperties.put(Constants.USER_FILE_BUFFER_BYTES , String.valueOf(Integer.MAX_VALUE) + "B");
    mCustomPropsTachyonConf = new TachyonConf(mProperties);
    Assert.assertEquals(Integer.MAX_VALUE,
            (int)mCustomPropsTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES));
    mCustomPropsTachyonConf.set(Constants.USER_FILE_BUFFER_BYTES,"1GB");
    Assert.assertEquals(1073741824,
            (int)mCustomPropsTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES));
  }
}
