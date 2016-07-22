/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Unit test for the {@link Configuration} class.
 */
public class ConfigurationTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();
  private static final String DEFAULT_HADOOP_UFS_PREFIX = "hdfs://,glusterfs:///";

  private static Map<String, String> sTestProperties = new LinkedHashMap<>();

  /**
   * Sets the properties and configuration before the test suite runs.
   */
  @BeforeClass
  public static void beforeClass() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests the default common properties.
   */
  @Test
  public void commonDefaultTest() {
    String alluxioHome = Configuration.get(PropertyKey.HOME);
    Assert.assertNotNull(alluxioHome);
    Assert.assertEquals("/mnt/alluxio_default_home", alluxioHome);

    String ufsAddress = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    Assert.assertNotNull(ufsAddress);
    Assert.assertEquals(alluxioHome + "/underFSStorage", ufsAddress);

    String value = Configuration.get(PropertyKey.WEB_RESOURCES);
    Assert.assertNotNull(value);
    Assert.assertEquals(alluxioHome + "/core/server/src/main/webapp", value);

    value = Configuration.get(PropertyKey.UNDERFS_HDFS_IMPL);
    Assert.assertNotNull(value);
    Assert.assertEquals("org.apache.hadoop.hdfs.DistributedFileSystem", value);

    value = Configuration.get(PropertyKey.UNDERFS_HDFS_PREFIXES);
    Assert.assertNotNull(value);
    Assert.assertEquals(DEFAULT_HADOOP_UFS_PREFIX, value);

    value = Configuration.get(PropertyKey.UNDERFS_GLUSTERFS_IMPL);
    Assert.assertNotNull(value);
    Assert.assertEquals("org.apache.hadoop.fs.glusterfs.GlusterFileSystem", value);

    boolean booleanValue = Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
    Assert.assertFalse(booleanValue);

    booleanValue = Configuration.getBoolean(PropertyKey.TEST_MODE);
    Assert.assertFalse(booleanValue);

    int intValue = Configuration.getInt(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    Assert.assertEquals(Constants.DEFAULT_HOST_RESOLUTION_TIMEOUT_MS, intValue);

    long longBytesValue =
        Configuration.getBytes(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
    Assert.assertEquals(Constants.MB * 8, longBytesValue);

    longBytesValue = Configuration.getBytes(PropertyKey.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX);
    Assert.assertEquals(Constants.MB * 16, longBytesValue);

    int maxTry = Configuration.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT);
    Assert.assertEquals(10, maxTry);
  }

  /**
   * Tests the default properties for the master.
   */
  @Test
  public void masterDefaultTest() {
    String alluxioHome = Configuration.get(PropertyKey.HOME);
    Assert.assertNotNull(alluxioHome);
    Assert.assertEquals("/mnt/alluxio_default_home", alluxioHome);

    String value = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    Assert.assertNotNull(value);
    Assert.assertEquals(alluxioHome + "/journal/", value);

    value = Configuration.get(PropertyKey.MASTER_HOSTNAME);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.getLocalHostName(100), value);

    value = Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    Assert.assertNotNull(value);
    Assert.assertEquals(Constants.FORMAT_FILE_PREFIX, value);

    value = Configuration.get(PropertyKey.MASTER_ADDRESS);
    Assert.assertNotNull(value);

    value = Configuration.get(PropertyKey.MASTER_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    int intValue = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
    Assert.assertEquals(19998, intValue);

    value = Configuration.get(PropertyKey.MASTER_WEB_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    intValue = Configuration.getInt(PropertyKey.MASTER_WEB_PORT);
    Assert.assertEquals(19999, intValue);

    intValue = Configuration.getInt(PropertyKey.WEB_THREADS);
    Assert.assertEquals(1, intValue);

    intValue = Configuration.getInt(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(Constants.SECOND_MS, intValue);

    intValue = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    Assert.assertEquals(512, intValue);

    intValue = Configuration.getInt(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
    Assert.assertEquals(300 * Constants.SECOND_MS, intValue);
  }

  /**
   * Tests the default properties for the worker.
   */
  @Test
  public void workerDefaultTest() {
    String value = Configuration.get(PropertyKey.WORKER_DATA_FOLDER);
    Assert.assertNotNull(value);
    Assert.assertEquals("/alluxioworker/", value);

    value = Configuration.get(PropertyKey.WORKER_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    int intValue = Configuration.getInt(PropertyKey.WORKER_RPC_PORT);
    Assert.assertEquals(29998, intValue);

    value = Configuration.get(PropertyKey.WORKER_DATA_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    intValue = Configuration.getInt(PropertyKey.WORKER_DATA_PORT);
    Assert.assertEquals(29999, intValue);

    value = Configuration.get(PropertyKey.WORKER_WEB_BIND_HOST);
    Assert.assertNotNull(value);
    Assert.assertEquals(NetworkAddressUtils.WILDCARD_ADDRESS, value);

    intValue = Configuration.getInt(PropertyKey.WORKER_WEB_PORT);
    Assert.assertEquals(30000, intValue);

    intValue = Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);
    Assert.assertEquals(10 * Constants.SECOND_MS, intValue);

    intValue = Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(Constants.SECOND_MS, intValue);

    intValue = Configuration.getInt(PropertyKey.WORKER_WORKER_BLOCK_THREADS_MIN);
    Assert.assertEquals(256, intValue);

    intValue = Configuration.getInt(PropertyKey.WORKER_SESSION_TIMEOUT_MS);
    Assert.assertEquals(10 * Constants.SECOND_MS, intValue);

    intValue = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);
    Assert.assertEquals(1, intValue);

    intValue = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
    Assert.assertEquals(0, intValue);

    long longValue = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Assert.assertEquals(Constants.GB, longValue);
  }

  /**
   * Tests the default properties for the user.
   */
  @Test
  public void userDefaultTest() {
    int intValue = Configuration.getInt(PropertyKey.USER_FAILED_SPACE_REQUEST_LIMITS);
    Assert.assertEquals(3, intValue);

    intValue = Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS);
    Assert.assertEquals(Constants.SECOND_MS, intValue);

    long longValue = Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
    Assert.assertEquals(Constants.MB, longValue);

    longValue = Configuration.getBytes(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
    Assert.assertEquals(8 * Constants.MB, longValue);
  }

  /**
   * Tests the simple substitution of variables.
   */
  @Test
  public void variableSubstitutionSimpleTest() {
    Configuration.set(PropertyKey.HOME, "testhome");
    Configuration.set(PropertyKey.LOGS_DIR, "${alluxio.home}/logs");
    Assert.assertEquals("testhome/logs", Configuration.get(PropertyKey.LOGS_DIR));

    Configuration.set(PropertyKey.MASTER_RPC_PORT, "8080");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "testhost");
    Configuration.set(PropertyKey.MASTER_ADDRESS,
        "alluxio://${alluixo.master.hostname}:${alluxio.master.port}");
    Assert.assertEquals("alluxio://testhost:8080", Configuration.get(PropertyKey.MASTER_ADDRESS));
  }

  /**
   * Tests the recursive substitution of variables.
   */
  @Test
  public void variableSubstitutionRecursiveTest() {
    Configuration.set(PropertyKey.HOME, "testhome");
    Configuration.set(PropertyKey.LOGS_DIR, "${alluxio.home}");
    Configuration.set(PropertyKey.SITE_CONF_DIR, "${alluxio.logs.dir}");

    Assert.assertEquals("testhome", Configuration.get(PropertyKey.SITE_CONF_DIR));
  }

  /**
   * Tests the substitution of system variables.
   */
  @Test
  public void systemVariableSubstitutionSampleTest() {
    // set system properties
    System.setProperty(PropertyKey.MASTER_HOSTNAME.toString(), "master");
    System.setProperty(PropertyKey.MASTER_RPC_PORT.toString(), "20001");
    System.setProperty(PropertyKey.ZOOKEEPER_ENABLED.toString(), "true");

    Configuration.defaultInit();
    String masterAddress = Configuration.get(PropertyKey.MASTER_ADDRESS);
    Assert.assertNotNull(masterAddress);
    Assert.assertEquals("alluxio-ft://master:20001", masterAddress);

    // clear system properties
    System.clearProperty(PropertyKey.MASTER_HOSTNAME.toString());
    System.clearProperty(PropertyKey.MASTER_RPC_PORT.toString());
    System.clearProperty(PropertyKey.ZOOKEEPER_ENABLED.toString());
  }

  /**
   * Tests that an exception is thrown when the {@link PropertyKey#USER_FILE_BUFFER_BYTES}
   * overflows.
   */
  @Test
  public void variableUserFileBufferBytesOverFlowCheckTest() {
    Properties mProperties = new Properties();
    mProperties.put(
        PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(Integer.MAX_VALUE + 1) + "B");
    mThrown.expect(IllegalArgumentException.class);
    Configuration.merge(mProperties);
  }

  /**
   * Tests that an exception is thrown when the {@link PropertyKey#USER_FILE_BUFFER_BYTES}
   * overflows.
   */
  @Test
  public void variableUserFileBufferBytesOverFlowCheckTest1() {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(PropertyKey.USER_FILE_BUFFER_BYTES.toString(),
        String.valueOf(Integer.MAX_VALUE + 1) + "B");
    mThrown.expect(IllegalArgumentException.class);
    Configuration.merge(properties);
  }

  /**
   * Tests that setting the {@link PropertyKey#USER_FILE_BUFFER_BYTES} runs correctly.
   */
  @Test
  public void variableUserFileBufferBytesNormalCheckTest() {
    Properties mProperties = new Properties();
    mProperties.put(PropertyKey.USER_FILE_BUFFER_BYTES , String.valueOf(Integer.MAX_VALUE) + "B");
    Configuration.merge(mProperties);
    Assert.assertEquals(Integer.MAX_VALUE,
        (int) Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
    Configuration.set(PropertyKey.USER_FILE_BUFFER_BYTES, "1GB");
    Assert.assertEquals(
        1073741824, (int) Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }
}
