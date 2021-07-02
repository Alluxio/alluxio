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

package alluxio.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.DefaultSupplier;
import alluxio.SystemPropertyRule;
import alluxio.TestLoggerRule;
import alluxio.conf.PropertyKey.Template;
import alluxio.test.util.CommonUtils;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Unit tests for the {@link alluxio.conf.InstancedConfiguration} class.
 */
public class InstancedConfigurationTest {

  private  InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public final TestLoggerRule mLogger = new TestLoggerRule();

  @Before
  public void before() {
    resetConf();
  }

  public void resetConf() {
    ConfigurationUtils.reloadProperties();
    mConfiguration = ConfigurationTestUtils.defaults();
  }

  @AfterClass
  public static void after() {
    ConfigurationUtils.reloadProperties();
  }

  @Test
  public void defaultLoggerCorrectlyLoaded() throws Exception {
    // Avoid interference from system properties. site-properties will not be loaded during tests
    try (Closeable p =
        new SystemPropertyRule(PropertyKey.LOGGER_TYPE.toString(), null).toResource()) {
      String loggerType = mConfiguration.get(PropertyKey.LOGGER_TYPE);
      assertEquals("Console", loggerType);
    }
  }

  @Test
  public void alias() throws Exception {
    try (Closeable p =
        new SystemPropertyRule("alluxio.master.worker.timeout.ms", "100").toResource()) {
      resetConf();
      assertEquals(100, mConfiguration.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS));
    }
  }

  @Test
  public void isSet() {
    assertFalse(mConfiguration.isSet(PropertyKey.ZOOKEEPER_ADDRESS));
    mConfiguration.set(PropertyKey.ZOOKEEPER_ADDRESS, "address");
    assertTrue(mConfiguration.isSet(PropertyKey.ZOOKEEPER_ADDRESS));
  }

  @Test
  public void isSetResolve() {
    mConfiguration.unset(PropertyKey.MASTER_HOSTNAME);
    mConfiguration.set(PropertyKey.MASTER_WEB_HOSTNAME, "${alluxio.master.hostname}");
    assertFalse(mConfiguration.isSet(PropertyKey.MASTER_WEB_HOSTNAME));
    mConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    assertTrue(mConfiguration.isSet(PropertyKey.MASTER_WEB_HOSTNAME));
  }

  @Test
  public void getInt() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "1");
    assertEquals(1, mConfiguration.getInt(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedIntThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "9448367483758473854738"); // bigger than MAX_INT
    mThrown.expect(RuntimeException.class);
    mConfiguration.getInt(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getLong() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "12345678910"); // bigger than MAX_INT
    assertEquals(12345678910L, mConfiguration.getLong(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedLongThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS,
        "999999999999999999999999999999999999"); // bigger than MAX_LONG
    mThrown.expect(RuntimeException.class);
    mConfiguration.getLong(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getDouble() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "1.1");
    assertEquals(1.1, mConfiguration.getDouble(PropertyKey.WEB_THREADS),
        /*tolerance=*/0.0001);
  }

  @Test
  public void getMalformedDoubleThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "1a");
    mThrown.expect(RuntimeException.class);
    mConfiguration.getDouble(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getFloat() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "1.1");
    assertEquals(1.1, mConfiguration.getFloat(PropertyKey.WEB_THREADS), /*tolerance=*/0.0001);
  }

  @Test
  public void getMalformedFloatThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "1a");
    mThrown.expect(RuntimeException.class);
    mConfiguration.getFloat(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getTrueBoolean() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "true");
    assertTrue(mConfiguration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getTrueBooleanUppercase() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "True");
    assertTrue(mConfiguration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getTrueBooleanMixcase() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "tRuE");
    assertTrue(mConfiguration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getFalseBoolean() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "false");
    assertFalse(mConfiguration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getFalseBooleanUppercase() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "False");
    assertFalse(mConfiguration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getFalseBooleanMixcase() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "fAlSe");
    assertFalse(mConfiguration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedBooleanThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "x");
    mThrown.expect(RuntimeException.class);
    mConfiguration.getBoolean(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getList() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "a,b,c");
    assertEquals(
        Lists.newArrayList("a", "b", "c"), mConfiguration.getList(PropertyKey.WEB_THREADS, ","));
  }

  private enum TestEnum {
    VALUE
  }

  @Test
  public void getEnum() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "VALUE");
    assertEquals(
        TestEnum.VALUE, mConfiguration.getEnum(PropertyKey.WEB_THREADS, TestEnum.class));
  }

  @Test
  public void getEnumDifferentCase() {
    // Keep configuration backwards compatible: ALLUXIO-3402
    mConfiguration.set(PropertyKey.WEB_THREADS, "Value");
    assertEquals(
        TestEnum.VALUE, mConfiguration.getEnum(PropertyKey.WEB_THREADS, TestEnum.class));
  }

  @Test
  public void getMalformedEnum() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "not_a_value");
    mThrown.expect(RuntimeException.class);
    mConfiguration.getEnum(PropertyKey.WEB_THREADS, TestEnum.class);
  }

  @Test
  public void getBytes() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10b");
    assertEquals(10, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesKb() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10kb");
    assertEquals(10 * Constants.KB, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesMb() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10mb");
    assertEquals(10 * Constants.MB, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesGb() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10gb");
    assertEquals(10 * (long) Constants.GB, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesGbUppercase() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10GB");
    assertEquals(10 * (long) Constants.GB, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesTb() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10tb");
    assertEquals(10 * Constants.TB, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytespT() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "10pb");
    assertEquals(10 * Constants.PB, mConfiguration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedBytesThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "100a");
    mThrown.expect(RuntimeException.class);
    mConfiguration.getBoolean(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getMs() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "100");
    assertEquals(100,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMS() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "100ms");
    assertEquals(100,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMillisecond() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "100millisecond");
    assertEquals(100,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsS() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10s");
    assertEquals(10 * Constants.SECOND,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsSUppercase() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10S");
    assertEquals(10 * Constants.SECOND,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsSEC() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10sec");
    assertEquals(10 * Constants.SECOND,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsSecond() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10second");
    assertEquals(10 * Constants.SECOND,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsM() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10m");
    assertEquals(10 * Constants.MINUTE,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMIN() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10min");
    assertEquals(10 * Constants.MINUTE,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMinute() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10minute");
    assertEquals(10 * Constants.MINUTE,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsH() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10h");
    assertEquals(10 * Constants.HOUR,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsHR() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10hr");
    assertEquals(10 * Constants.HOUR,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsHour() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10hour");
    assertEquals(10 * Constants.HOUR,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsD() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10d");
    assertEquals(10 * Constants.DAY,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsDay() {
    mConfiguration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10day");
    assertEquals(10 * Constants.DAY,
        mConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getNegativeSyncInterval() {
    mConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1");
    assertEquals(-1, mConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getNegativeSyncIntervalS() {
    mConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1s");
    assertTrue(mConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL) < 0);
  }

  @Test
  public void getZeroSyncInterval() {
    mConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0");
    assertEquals(0, mConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getZeroSyncIntervalS() {
    mConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0s");
    assertEquals(0, mConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getPositiveSyncInterval() {
    mConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "10");
    assertEquals(10, mConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getPosiviteSyncIntervalS() {
    mConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "10s");
    assertEquals(10 * Constants.SECOND_MS,
        mConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getUnsetValueThrowsException() {
    mThrown.expect(RuntimeException.class);
    mConfiguration.get(PropertyKey.S3A_ACCESS_KEY);
  }

  @Test
  public void getNestedProperties() {
    mConfiguration.set(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY.format("foo",
            PropertyKey.WEB_THREADS.toString()), "val1");
    mConfiguration.set(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY.format("foo",
            "alluxio.unknown.property"), "val2");
    Map<String, String> expected = new HashMap<>();
    expected.put(PropertyKey.WEB_THREADS.toString(), "val1");
    expected.put("alluxio.unknown.property", "val2");
    assertThat(mConfiguration.getNestedProperties(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("foo")),
        CoreMatchers.is(expected));
  }

  @Test
  public void getNestedPropertiesEmptyTrailingProperty() {
    mConfiguration.set(PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY
        .format("foo", ""), "val");
    Map<String, String> empty = new HashMap<>();
    assertThat(mConfiguration.getNestedProperties(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("foo")),
        CoreMatchers.is(empty));
  }

  @Test
  public void getNestedPropertiesWrongPrefix() {
    mConfiguration.set(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY.format("foo",
            PropertyKey.WEB_THREADS.toString()),
        "val");
    Map<String, String> empty = new HashMap<>();
    assertThat(mConfiguration.getNestedProperties(PropertyKey.HOME),
        CoreMatchers.is(empty));
    assertThat(mConfiguration.getNestedProperties(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("bar")),
        CoreMatchers.is(empty));
  }

  @Test
  public void getClassTest() { // The name getClass is already reserved.
    mConfiguration.set(PropertyKey.WEB_THREADS, "java.lang.String");
    assertEquals(String.class, mConfiguration.getClass(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedClassThrowsException() {
    mConfiguration.set(PropertyKey.WEB_THREADS, "java.util.not.a.class");
    mThrown.expect(RuntimeException.class);
    mConfiguration.getClass(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getTemplatedKey() {
    mConfiguration.set(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS, "test");
    assertEquals("test",
        mConfiguration.get(PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(0)));
  }

  @Test
  public void variableSubstitution() {
    mConfiguration.merge(ImmutableMap.of(
        PropertyKey.WORK_DIR, "value",
        PropertyKey.LOGS_DIR, "${alluxio.work.dir}/logs"),
        Source.SYSTEM_PROPERTY);
    String substitution = mConfiguration.get(PropertyKey.LOGS_DIR);
    assertEquals("value/logs", substitution);
  }

  @Test
  public void twoVariableSubstitution() {
    mConfiguration.merge(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, "value1",
        PropertyKey.MASTER_RPC_PORT, "value2",
        PropertyKey.MASTER_JOURNAL_FOLDER, "${alluxio.master.hostname}-${alluxio.master.rpc.port}"),
        Source.SYSTEM_PROPERTY);
    String substitution = mConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    assertEquals("value1-value2", substitution);
  }

  @Test
  public void recursiveVariableSubstitution() {
    mConfiguration.merge(ImmutableMap.of(
        PropertyKey.WORK_DIR, "value",
        PropertyKey.LOGS_DIR, "${alluxio.work.dir}/logs",
        PropertyKey.SITE_CONF_DIR, "${alluxio.logs.dir}/conf"),
        Source.SYSTEM_PROPERTY);
    String substitution2 = mConfiguration.get(PropertyKey.SITE_CONF_DIR);
    assertEquals("value/logs/conf", substitution2);
  }

  @Test
  public void systemVariableSubstitution() throws Exception {
    try (Closeable p =
        new SystemPropertyRule(PropertyKey.MASTER_HOSTNAME.toString(), "new_master").toResource()) {

      resetConf();
      assertEquals("new_master", mConfiguration.get(PropertyKey.MASTER_HOSTNAME));
    }
  }

  @Test
  public void systemPropertySubstitution() throws Exception {
    try (Closeable p = new SystemPropertyRule("user.home", "/home").toResource()) {
      resetConf();
      mConfiguration.set(PropertyKey.WORK_DIR, "${user.home}/work");
      assertEquals("/home/work", mConfiguration.get(PropertyKey.WORK_DIR));
    }
  }

  @Test
  public void circularSubstitution() throws Exception {
    mConfiguration.set(PropertyKey.HOME, String.format("${%s}", PropertyKey.HOME.toString()));
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(PropertyKey.HOME.toString());
    mConfiguration.get(PropertyKey.HOME);
  }

  @Test
  public void userFileBufferBytesOverFlowException() {
    mConfiguration.set(PropertyKey.USER_FILE_BUFFER_BYTES,
        String.valueOf(Integer.MAX_VALUE + 1) + "B");
    mThrown.expect(IllegalStateException.class);
    mConfiguration.validate();
  }

  @Test
  public void shortMasterHeartBeatTimeout() {
    mConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "5min");
    mConfiguration.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "4min");
    mThrown.expect(IllegalStateException.class);
    mConfiguration.validate();
  }

  @Test
  public void setUserFileBufferBytesMaxInteger() {
    mConfiguration.set(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(Integer.MAX_VALUE) + "B");
    assertEquals(Integer.MAX_VALUE,
        (int) mConfiguration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }

  @Test
  public void setUserFileBufferBytes1GB() {
    mConfiguration.set(PropertyKey.USER_FILE_BUFFER_BYTES, "1GB");
    assertEquals(1073741824,
        (int) mConfiguration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }

  @Test
  public void unset() {
    assertFalse(mConfiguration.isSet(PropertyKey.SECURITY_LOGIN_USERNAME));
    mConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "test");
    assertTrue(mConfiguration.isSet(PropertyKey.SECURITY_LOGIN_USERNAME));
    mConfiguration.unset(PropertyKey.SECURITY_LOGIN_USERNAME);
    assertFalse(mConfiguration.isSet(PropertyKey.SECURITY_LOGIN_USERNAME));
  }

  @Test
  public void validateTieredLocality() throws Exception {
    // Pre-load the Configuration class so that the exception is thrown when we call init(), not
    // during class loading.
    resetConf();
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(Template.LOCALITY_TIER.format("unknownTier").toString(), "val");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      mThrown.expect(IllegalStateException.class);
      mThrown.expectMessage("Tier unknownTier is configured by alluxio.locality.unknownTier, but "
          + "does not exist in the tier list [node, rack] configured by alluxio.locality.order");
      resetConf();
    }
  }

  @Test
  public void propertyTestModeEqualsTrue() throws Exception {
    assertTrue(mConfiguration.getBoolean(PropertyKey.TEST_MODE));
  }

  @Test
  public void sitePropertiesNotLoadedInTest() throws Exception {
    Properties props = new Properties();
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "TEST_LOGGER");
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    props.store(new FileOutputStream(propsFile), "ignored header");
    // Avoid interference from system properties. Reset SITE_CONF_DIR to include the temp
    // site-properties file
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.LOGGER_TYPE.toString(), null);
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getCanonicalPath());
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      mConfiguration = ConfigurationTestUtils.defaults();
      assertEquals(PropertyKey.LOGGER_TYPE.getDefaultValue(),
          mConfiguration.get(PropertyKey.LOGGER_TYPE));
    }
  }

  @Test
  public void sitePropertiesLoadedNotInTest() throws Exception {
    Properties props = new Properties();
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "TEST_LOGGER");
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    props.store(new FileOutputStream(propsFile), "ignored header");
    // Avoid interference from system properties. Reset SITE_CONF_DIR to include the temp
    // site-properties file
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.LOGGER_TYPE.toString(), null);
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    sysProps.put(PropertyKey.TEST_MODE.toString(), "false");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      resetConf();
      assertEquals("TEST_LOGGER", mConfiguration.get(PropertyKey.LOGGER_TYPE));
    }
  }

  @Test
  public void setIgnoredPropertiesInSiteProperties() throws Exception {
    resetConf();
    Properties siteProps = new Properties();
    siteProps.setProperty(PropertyKey.LOGS_DIR.toString(), "/tmp/logs1");
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    siteProps.store(new FileOutputStream(propsFile), "tmp site properties file");
    Map<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    sysProps.put(PropertyKey.TEST_MODE.toString(), "false");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      mThrown.expect(IllegalStateException.class);
      resetConf();
    }
  }

  @Test
  public void setIgnoredPropertiesInSystemProperties() throws Exception {
    Properties siteProps = new Properties();
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    siteProps.store(new FileOutputStream(propsFile), "tmp site properties file");
    Map<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.LOGS_DIR.toString(), "/tmp/logs1");
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    sysProps.put(PropertyKey.TEST_MODE.toString(), "false");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      resetConf();
      assertEquals(
          Source.SYSTEM_PROPERTY, mConfiguration.getSource(PropertyKey.LOGS_DIR));
      assertEquals("/tmp/logs1", mConfiguration.get(PropertyKey.LOGS_DIR));
    }
  }

  @Test
  public void noWhitespaceTrailingInSiteProperties() throws Exception {
    Properties siteProps = new Properties();
    siteProps.setProperty(PropertyKey.MASTER_HOSTNAME.toString(), " host-1 ");
    siteProps.setProperty(PropertyKey.WEB_THREADS.toString(), "\t123\t");
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    siteProps.store(new FileOutputStream(propsFile), "tmp site properties file");
    // Avoid interference from system properties. Reset SITE_CONF_DIR to include the temp
    // site-properties file
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    sysProps.put(PropertyKey.TEST_MODE.toString(), "false");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      resetConf();
      assertEquals("host-1", mConfiguration.get(PropertyKey.MASTER_HOSTNAME));
      assertEquals("123", mConfiguration.get(PropertyKey.WEB_THREADS));
    }
  }

  @Test
  public void source() throws Exception {
    Properties siteProps = new Properties();
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    siteProps.setProperty(PropertyKey.MASTER_HOSTNAME.toString(), "host-1");
    siteProps.setProperty(PropertyKey.MASTER_WEB_PORT.toString(), "1234");
    siteProps.store(new FileOutputStream(propsFile), "tmp site properties file");
    Map<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.LOGS_DIR.toString(), "/tmp/logs1");
    sysProps.put(PropertyKey.MASTER_WEB_PORT.toString(), "4321");
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    sysProps.put(PropertyKey.TEST_MODE.toString(), "false");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      resetConf();
      // set only in site prop
      assertEquals(Source.Type.SITE_PROPERTY,
          mConfiguration.getSource(PropertyKey.MASTER_HOSTNAME).getType());
      // set both in site and system prop
      assertEquals(Source.SYSTEM_PROPERTY,
          mConfiguration.getSource(PropertyKey.MASTER_WEB_PORT));
      // set only in system prop
      assertEquals(Source.SYSTEM_PROPERTY,
          mConfiguration.getSource(PropertyKey.LOGS_DIR));
      // set neither in system prop
      assertEquals(Source.DEFAULT,
          mConfiguration.getSource(PropertyKey.MASTER_RPC_PORT));
    }
  }

  @Test
  public void getRuntimeDefault() throws Exception {
    AtomicInteger x = new AtomicInteger(100);
    PropertyKey key = new PropertyKey.Builder("testKey")
        .setDefaultSupplier(new DefaultSupplier(() -> x.get(), "finds x"))
        .build();
    assertEquals(100, mConfiguration.getInt(key));
    x.set(20);
    assertEquals(20, mConfiguration.getInt(key));
  }

  @Test
  public void toMap() throws Exception {
    // Create a nested property to test
    String testKeyName = "alluxio.extensions.dir";
    PropertyKey nestedKey = PropertyKey.SECURITY_LOGIN_USERNAME;
    String nestedValue = String.format("${%s}.test", testKeyName);
    mConfiguration.set(nestedKey, nestedValue);

    Map<String, String> resolvedMap = mConfiguration.toMap();

    // Test if the value of the created nested property is correct
    assertEquals(mConfiguration.get(PropertyKey.fromString(testKeyName)),
        resolvedMap.get(testKeyName));
    String nestedResolvedValue = String.format("%s.test", resolvedMap.get(testKeyName));
    assertEquals(nestedResolvedValue, resolvedMap.get(nestedKey.toString()));

    // Test if the values in the resolvedMap is resolved
    String resolvedValue1 = String.format("%s/extensions", resolvedMap.get("alluxio.home"));
    assertEquals(resolvedValue1, resolvedMap.get(testKeyName));

    String resolvedValue2 =  String.format("%s/logs", resolvedMap.get("alluxio.work.dir"));
    assertEquals(resolvedValue2, resolvedMap.get("alluxio.logs.dir"));

    // Test if the resolvedMap include all kinds of properties
    assertTrue(resolvedMap.containsKey("alluxio.debug"));
    assertTrue(resolvedMap.containsKey("alluxio.fuse.fs.name"));
    assertTrue(resolvedMap.containsKey("alluxio.master.journal.folder"));
    assertTrue(resolvedMap.containsKey("alluxio.proxy.web.port"));
    assertTrue(resolvedMap.containsKey("alluxio.security.authentication.type"));
    assertTrue(resolvedMap.containsKey("alluxio.user.block.master.client.pool.size.max"));
    assertTrue(resolvedMap.containsKey("alluxio.worker.bind.host"));
  }

  @Test
  public void toRawMap() throws Exception {
    // Create a nested property to test
    PropertyKey testKey = PropertyKey.SECURITY_LOGIN_USERNAME;
    String testValue = String.format("${%s}.test", "alluxio.extensions.dir");
    mConfiguration.set(testKey, testValue);

    Map<String, String> rawMap =
        mConfiguration.toMap(ConfigurationValueOptions.defaults().useRawValue(true));

    // Test if the value of the created nested property remains raw
    assertEquals(testValue, rawMap.get(testKey.toString()));

    // Test if some value in raw map is of ${VALUE} format
    String regexString = "(\\$\\{([^{}]*)\\})";
    Pattern confRegex = Pattern.compile(regexString);
    assertTrue(confRegex.matcher(rawMap.get("alluxio.logs.dir")).find());
  }

  @Test
  public void getCredentialsDisplayValue() {
    PropertyKey testKey = PropertyKey.S3A_SECRET_KEY;
    String testValue = "12345";
    assertEquals(PropertyKey.DisplayType.CREDENTIALS, testKey.getDisplayType());
    mConfiguration.set(testKey, testValue);

    assertNotEquals(testValue, mConfiguration.get(testKey,
        ConfigurationValueOptions.defaults().useDisplayValue(true)));
    assertNotEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true))
        .get(testKey.getName()));
  }

  @Test
  public void getDefaultDisplayValue() {
    PropertyKey testKey = PropertyKey.SECURITY_LOGIN_USERNAME;
    String testValue = "12345";
    assertEquals(PropertyKey.DisplayType.DEFAULT, testKey.getDisplayType());
    mConfiguration.set(testKey, testValue);

    assertEquals(testValue, mConfiguration.get(testKey,
        ConfigurationValueOptions.defaults().useDisplayValue(true)));
    assertEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true))
        .get(testKey.getName()));
  }

  @Test
  public void getNestedCredentialsDisplayValue() {
    PropertyKey nestedProperty =
        PropertyKey.fromString("alluxio.master.journal.ufs.option.aws.secretKey");
    String testValue = "12345";
    mConfiguration.set(nestedProperty, testValue);

    assertNotEquals(testValue, mConfiguration.get(nestedProperty,
        ConfigurationValueOptions.defaults().useDisplayValue(true)));
    assertNotEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true))
        .get(nestedProperty.getName()));
    assertNotEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(true))
        .get(nestedProperty.getName()));
  }

  @Test
  public void getNestedDefaultDisplayValue() {
    PropertyKey nestedProperty = PropertyKey.fromString(
        "alluxio.master.journal.ufs.option.alluxio.underfs.hdfs.configuration");
    String testValue = "conf/core-site.xml:conf/hdfs-site.xml";
    mConfiguration.set(nestedProperty, testValue);

    assertEquals(testValue, mConfiguration.get(nestedProperty,
        ConfigurationValueOptions.defaults().useDisplayValue(true)));
    assertEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true))
        .get(nestedProperty.getName()));
    assertEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(true))
        .get(nestedProperty.getName()));
  }

  @Test
  public void getTemplateCredentialsDisplayValue() {
    PropertyKey templateProperty = PropertyKey.fromString(
        "fs.azure.account.key.someone.blob.core.windows.net");
    String testValue = "12345";
    mConfiguration.set(templateProperty, testValue);

    assertNotEquals(testValue, mConfiguration.get(templateProperty,
        ConfigurationValueOptions.defaults().useDisplayValue(true)));
    assertNotEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true))
        .get(templateProperty.getName()));
    assertNotEquals(testValue, mConfiguration.toMap(
        ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(true))
        .get(templateProperty.getName()));
  }

  @Test
  public void getCredentialsDisplayValueIdentical() {
    PropertyKey testKey = PropertyKey.S3A_SECRET_KEY;
    String testValue = "12345";
    assertEquals(PropertyKey.DisplayType.CREDENTIALS, testKey.getDisplayType());

    mConfiguration.set(testKey, testValue);
    String displayValue1 = mConfiguration.get(testKey,
        ConfigurationValueOptions.defaults().useDisplayValue(true));

    String testValue2 = "abc";
    mConfiguration.set(testKey, testValue2);

    String displayValue2 = mConfiguration.get(testKey,
        ConfigurationValueOptions.defaults().useDisplayValue(true));
    assertEquals(displayValue1, displayValue2);
  }

  @Test
  public void extensionProperty() {
    // simulate the case a ext key is picked by site property, unrecognized
    String fakeKeyName = "fake.extension.key";
    mConfiguration.merge(ImmutableMap.of(fakeKeyName, "value"), Source.siteProperty("ignored"));
    assertFalse(PropertyKey.fromString(fakeKeyName).isBuiltIn());
    // simulate the case the same key is built again inside the extension
    PropertyKey fakeExtensionKey = new PropertyKey.Builder(fakeKeyName).build();
    assertEquals("value", mConfiguration.get(fakeExtensionKey));
    assertTrue(PropertyKey.fromString(fakeKeyName).isBuiltIn());
  }

  @Test
  public void findPropertiesFileClasspath() throws Exception {
    try (Closeable p =
        new SystemPropertyRule(PropertyKey.TEST_MODE.toString(), "false").toResource()) {
      File dir = AlluxioTestDirectory.createTemporaryDirectory("findPropertiesFileClasspath");
      CommonUtils.classLoadURL(dir.getCanonicalPath());
      File props = new File(dir, "alluxio-site.properties");

      try (BufferedWriter writer = Files.newBufferedWriter(props.toPath())) {
        writer.write(String.format("%s=%s", PropertyKey.MASTER_HOSTNAME, "test_hostname"));
      }
      resetConf();
      assertEquals("test_hostname", mConfiguration.get(PropertyKey.MASTER_HOSTNAME));
      assertEquals(Source.siteProperty(props.getCanonicalPath()),
          mConfiguration.getSource(PropertyKey.MASTER_HOSTNAME));
      props.delete();
    }
  }

  @Test
  public void noPropertiesAnywhere() throws Exception {
    try (Closeable p =
             new SystemPropertyRule(PropertyKey.TEST_MODE.toString(), "false").toResource()) {
      mConfiguration.unset(PropertyKey.SITE_CONF_DIR);
      resetConf();
      assertEquals("0.0.0.0", mConfiguration.get(PropertyKey.PROXY_WEB_BIND_HOST));
    }
  }

  @Test
  public void initConfWithExtenstionProperty() throws Exception {
    try (Closeable p = new SystemPropertyRule("alluxio.master.journal.ufs.option.fs.obs.endpoint",
        "foo").toResource()) {
      resetConf();
      assertEquals("foo",
          mConfiguration.get(Template.MASTER_JOURNAL_UFS_OPTION_PROPERTY
              .format("fs.obs.endpoint")));
    }
  }

  @Test
  public void validateDefaultConfiguration() {
    mConfiguration.validate();
  }

  @Test
  public void removedKeyThrowsException() {
    try {
      mConfiguration.set(PropertyKey.fromString(RemovedKey.Name.TEST_REMOVED_KEY),
          "true");
      mConfiguration.validate();
      fail("Should have thrown a runtime exception when validating with a removed key");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(
          String.format("%s is no longer a valid property",
              RemovedKey.Name.TEST_REMOVED_KEY)));
    }
    mConfiguration = ConfigurationTestUtils.defaults();
    try {
      mConfiguration.set(PropertyKey.fromString(RemovedKey.Name.TEST_REMOVED_KEY), "true");
      mConfiguration.validate();
      fail("Should have thrown a runtime exception when validating with a removed key");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(
          String.format("%s is no longer a valid property",
              RemovedKey.Name.TEST_REMOVED_KEY)));
    }
  }

  @Test
  public void testDeprecatedKey() {
    mConfiguration.set(PropertyKey.TEST_DEPRECATED_KEY, "true");
    mConfiguration.validate();
    String logString = String.format("%s is deprecated", PropertyKey.TEST_DEPRECATED_KEY);
    assertTrue(mLogger.wasLogged(logString));
    assertEquals(1, mLogger.logCount(logString));
  }

  @Test
  public void testDeprecatedKeysNotLogged() {
    mConfiguration.validate();
    assertFalse(mLogger.wasLogged(" is deprecated"));
  }

  @Test
  public void unknownTieredStorageAlias() throws Exception {
    for (String alias : Arrays.asList("mem", "ssd", "hdd", "unknown")) {
      try (Closeable p = new SystemPropertyRule("alluxio.worker.tieredstore.level0.alias", alias)
          .toResource()) {
        resetConf();
        mConfiguration.validate();
        fail("Should have thrown a runtime exception when using an unknown tier alias");
      } catch (RuntimeException e) {
        assertTrue(e.getMessage().contains(
            String.format("Alias \"%s\" on tier 0 on worker (configured by %s) is not found "
                + "in global tiered", alias, Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0))
        ));
      }
    }
  }

  @Test
  public void wrongTieredStorageLevel() throws Exception {
    try (Closeable p =
             new SystemPropertyRule(ImmutableMap.of("alluxio.master.tieredstore.global.levels", "1",
                 "alluxio.worker.tieredstore.levels", "2")).toResource()) {
      resetConf();
      mConfiguration.validate();
      fail("Should have thrown a runtime exception when setting an unknown tier level");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(
          String.format("%s tiers on worker (configured by %s), larger than global %s tiers "
                  + "(configured by %s) ", 2, PropertyKey.WORKER_TIERED_STORE_LEVELS, 1,
              PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS)));
    }
  }
}
