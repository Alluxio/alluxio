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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import alluxio.Configuration.Source;
import alluxio.PropertyKey.Template;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Unit tests for the {@link Configuration} class.
 */
public class ConfigurationTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder mFolder = new TemporaryFolder();

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void defaultLoggerCorrectlyLoaded() throws Exception {
    // Avoid interference from system properties. site-properties will not be loaded during tests
    try (Closeable p =
        new SystemPropertyRule(PropertyKey.LOGGER_TYPE.toString(), null).toResource()) {
      String loggerType = Configuration.get(PropertyKey.LOGGER_TYPE);
      assertEquals("Console", loggerType);
    }
  }

  @Test
  public void alias() {
    Configuration.merge(ImmutableMap.of("alluxio.master.worker.timeout.ms", "100"),
        Source.SYSTEM_PROPERTY);
    assertEquals(100, Configuration.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS));
  }

  @Test
  public void getInt() {
    Configuration.set(PropertyKey.WEB_THREADS, "1");
    assertEquals(1, Configuration.getInt(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedIntThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS, "9448367483758473854738"); // bigger than MAX_INT
    mThrown.expect(RuntimeException.class);
    Configuration.getInt(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getLong() {
    Configuration.set(PropertyKey.WEB_THREADS, "12345678910"); // bigger than MAX_INT
    assertEquals(12345678910L, Configuration.getLong(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedLongThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS,
        "999999999999999999999999999999999999"); // bigger than MAX_LONG
    mThrown.expect(RuntimeException.class);
    Configuration.getLong(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getDouble() {
    Configuration.set(PropertyKey.WEB_THREADS, "1.1");
    assertEquals(1.1, Configuration.getDouble(PropertyKey.WEB_THREADS),
        /*tolerance=*/0.0001);
  }

  @Test
  public void getMalformedDoubleThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS, "1a");
    mThrown.expect(RuntimeException.class);
    Configuration.getDouble(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getFloat() {
    Configuration.set(PropertyKey.WEB_THREADS, "1.1");
    assertEquals(1.1, Configuration.getFloat(PropertyKey.WEB_THREADS), /*tolerance=*/0.0001);
  }

  @Test
  public void getMalformedFloatThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS, "1a");
    mThrown.expect(RuntimeException.class);
    Configuration.getFloat(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getTrueBoolean() {
    Configuration.set(PropertyKey.WEB_THREADS, "true");
    assertTrue(Configuration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getTrueBooleanUppercase() {
    Configuration.set(PropertyKey.WEB_THREADS, "True");
    assertTrue(Configuration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getTrueBooleanMixcase() {
    Configuration.set(PropertyKey.WEB_THREADS, "tRuE");
    assertTrue(Configuration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getFalseBoolean() {
    Configuration.set(PropertyKey.WEB_THREADS, "false");
    assertFalse(Configuration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getFalseBooleanUppercase() {
    Configuration.set(PropertyKey.WEB_THREADS, "False");
    assertFalse(Configuration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getFalseBooleanMixcase() {
    Configuration.set(PropertyKey.WEB_THREADS, "fAlSe");
    assertFalse(Configuration.getBoolean(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedBooleanThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS, "x");
    mThrown.expect(RuntimeException.class);
    Configuration.getBoolean(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getList() {
    Configuration.set(PropertyKey.WEB_THREADS, "a,b,c");
    assertEquals(
        Lists.newArrayList("a", "b", "c"), Configuration.getList(PropertyKey.WEB_THREADS, ","));
  }

  private enum TestEnum {
    VALUE
  }

  @Test
  public void getEnum() {
    Configuration.set(PropertyKey.WEB_THREADS, "VALUE");
    assertEquals(
        TestEnum.VALUE, Configuration.getEnum(PropertyKey.WEB_THREADS, TestEnum.class));
  }

  @Test
  public void getMalformedEnum() {
    Configuration.set(PropertyKey.WEB_THREADS, "not_a_value");
    mThrown.expect(RuntimeException.class);
    Configuration.getEnum(PropertyKey.WEB_THREADS, TestEnum.class);
  }

  @Test
  public void getBytes() {
    Configuration.set(PropertyKey.WEB_THREADS, "10b");
    assertEquals(10, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesKb() {
    Configuration.set(PropertyKey.WEB_THREADS, "10kb");
    assertEquals(10 * Constants.KB, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesMb() {
    Configuration.set(PropertyKey.WEB_THREADS, "10mb");
    assertEquals(10 * Constants.MB, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesGb() {
    Configuration.set(PropertyKey.WEB_THREADS, "10gb");
    assertEquals(10 * (long) Constants.GB, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesGbUppercase() {
    Configuration.set(PropertyKey.WEB_THREADS, "10GB");
    assertEquals(10 * (long) Constants.GB, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytesTb() {
    Configuration.set(PropertyKey.WEB_THREADS, "10tb");
    assertEquals(10 * Constants.TB, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getBytespT() {
    Configuration.set(PropertyKey.WEB_THREADS, "10pb");
    assertEquals(10 * Constants.PB, Configuration.getBytes(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedBytesThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS, "100a");
    mThrown.expect(RuntimeException.class);
    Configuration.getBoolean(PropertyKey.WEB_THREADS);
  }

  @Test
  public void getMs() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "100");
    assertEquals(100,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMS() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "100ms");
    assertEquals(100,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMillisecond() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "100millisecond");
    assertEquals(100,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsS() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10s");
    assertEquals(10 * Constants.SECOND,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsSUppercase() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10S");
    assertEquals(10 * Constants.SECOND,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsSEC() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10sec");
    assertEquals(10 * Constants.SECOND,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsSecond() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10second");
    assertEquals(10 * Constants.SECOND,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsM() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10m");
    assertEquals(10 * Constants.MINUTE,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMIN() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10min");
    assertEquals(10 * Constants.MINUTE,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsMinute() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10minute");
    assertEquals(10 * Constants.MINUTE,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsH() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10h");
    assertEquals(10 * Constants.HOUR,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsHR() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10hr");
    assertEquals(10 * Constants.HOUR,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsHour() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10hour");
    assertEquals(10 * Constants.HOUR,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsD() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10d");
    assertEquals(10 * Constants.DAY,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getMsDay() {
    Configuration.set(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS, "10day");
    assertEquals(10 * Constants.DAY,
        Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
  }

  @Test
  public void getNegativeSyncInterval() {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1");
    assertEquals(-1, Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getNegativeSyncIntervalS() {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1s");
    assertTrue(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL) < 0);
  }

  @Test
  public void getZeroSyncInterval() {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0");
    assertEquals(0, Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getZeroSyncIntervalS() {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0s");
    assertEquals(0, Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getPositiveSyncInterval() {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "10");
    assertEquals(10, Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getPosiviteSyncIntervalS() {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "10s");
    assertEquals(10 * Constants.SECOND_MS,
        Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
  }

  @Test
  public void getNestedProperties() {
    Configuration.set(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY.format("foo",
            PropertyKey.WEB_THREADS.toString()), "val1");
    Configuration.set(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY.format("foo",
            "alluxio.unknown.property"), "val2");
    Map<String, String> expected = new HashMap<>();
    expected.put(PropertyKey.WEB_THREADS.toString(), "val1");
    expected.put("alluxio.unknown.property", "val2");
    assertThat(Configuration.getNestedProperties(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("foo")),
        CoreMatchers.is(expected));
  }

  @Test
  public void getNestedPropertiesEmptyTrailingProperty() {
    Configuration.set(PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY
        .format("foo", ""), "val");
    Map<String, String> empty = new HashMap<>();
    assertThat(Configuration.getNestedProperties(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("foo")),
        CoreMatchers.is(empty));
  }

  @Test
  public void getNestedPropertiesWrongPrefix() {
    Configuration.set(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION_PROPERTY.format("foo",
            PropertyKey.WEB_THREADS.toString()),
        "val");
    Map<String, String> empty = new HashMap<>();
    assertThat(Configuration.getNestedProperties(PropertyKey.HOME),
        CoreMatchers.is(empty));
    assertThat(Configuration.getNestedProperties(
        PropertyKey.Template.MASTER_MOUNT_TABLE_OPTION.format("bar")),
        CoreMatchers.is(empty));
  }

  @Test
  public void getClassTest() { // The name getClass is already reserved.
    Configuration.set(PropertyKey.WEB_THREADS, "java.lang.String");
    assertEquals(String.class, Configuration.getClass(PropertyKey.WEB_THREADS));
  }

  @Test
  public void getMalformedClassThrowsException() {
    Configuration.set(PropertyKey.WEB_THREADS, "java.util.not.a.class");
    mThrown.expect(RuntimeException.class);
    Configuration.getClass(PropertyKey.WEB_THREADS);
  }

  @Test
  public void variableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        PropertyKey.WORK_DIR, "value",
        PropertyKey.LOGS_DIR, "${alluxio.work.dir}/logs"),
        Source.SYSTEM_PROPERTY);
    String substitution = Configuration.get(PropertyKey.LOGS_DIR);
    assertEquals("value/logs", substitution);
  }

  @Test
  public void twoVariableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, "value1",
        PropertyKey.MASTER_RPC_PORT, "value2",
        PropertyKey.MASTER_JOURNAL_FOLDER, "${alluxio.master.hostname}-${alluxio.master.port}"),
        Source.SYSTEM_PROPERTY);
    String substitution = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    assertEquals("value1-value2", substitution);
  }

  @Test
  public void recursiveVariableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        PropertyKey.WORK_DIR, "value",
        PropertyKey.LOGS_DIR, "${alluxio.work.dir}/logs",
        PropertyKey.SITE_CONF_DIR, "${alluxio.logs.dir}/conf"),
        Source.SYSTEM_PROPERTY);
    String substitution2 = Configuration.get(PropertyKey.SITE_CONF_DIR);
    assertEquals("value/logs/conf", substitution2);
  }

  @Test
  public void systemVariableSubstitution() throws Exception {
    try (Closeable p =
        new SystemPropertyRule(PropertyKey.MASTER_HOSTNAME.toString(), "new_master").toResource()) {
      Configuration.init();
      assertEquals("new_master", Configuration.get(PropertyKey.MASTER_HOSTNAME));
    }
  }

  @Test
  public void systemPropertySubstitution() throws Exception {
    try (Closeable p = new SystemPropertyRule("user.home", "/home").toResource()) {
      Configuration.init();
      Configuration.merge(ImmutableMap.of(PropertyKey.WORK_DIR, "${user.home}/work"),
          Source.SITE_PROPERTY);
      assertEquals("/home/work", Configuration.get(PropertyKey.WORK_DIR));
    }
  }

  @Test
  public void circularSubstitution() throws Exception {
    Configuration.merge(
        ImmutableMap.of(PropertyKey.HOME, String.format("${%s}", PropertyKey.HOME.toString())),
        Source.SITE_PROPERTY);
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(PropertyKey.HOME.toString());
    Configuration.get(PropertyKey.HOME);
  }

  @Test
  public void userFileBufferBytesOverFlowException() {
    Configuration.set(PropertyKey.USER_FILE_BUFFER_BYTES,
        String.valueOf(Integer.MAX_VALUE + 1) + "B");
    mThrown.expect(IllegalStateException.class);
    Configuration.validate();
  }

  @Test
  public void setUserFileBufferBytesMaxInteger() {
    Configuration.set(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(Integer.MAX_VALUE) + "B");
    assertEquals(Integer.MAX_VALUE,
        (int) Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }

  @Test
  public void setUserFileBufferBytes1GB() {
    Configuration.set(PropertyKey.USER_FILE_BUFFER_BYTES, "1GB");
    assertEquals(1073741824,
        (int) Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }

  @Test
  public void unset() {
    assertFalse(Configuration.containsKey(PropertyKey.SECURITY_LOGIN_USERNAME));
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "test");
    assertTrue(Configuration.containsKey(PropertyKey.SECURITY_LOGIN_USERNAME));
    Configuration.unset(PropertyKey.SECURITY_LOGIN_USERNAME);
    assertFalse(Configuration.containsKey(PropertyKey.SECURITY_LOGIN_USERNAME));
  }

  @Test
  public void validateTieredLocality() throws Exception {
    // Pre-load the Configuration class so that the exception is thrown when we call init(), not
    // during class loading.
    Configuration.init();
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put(Template.LOCALITY_TIER.format("unknownTier").toString(), "val");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      mThrown.expect(IllegalStateException.class);
      mThrown.expectMessage("Tier unknownTier is configured by alluxio.locality.unknownTier, but "
          + "does not exist in the tier list [node, rack] configured by alluxio.locality.order");
      Configuration.init();
    }
  }

  @Test
  public void propertyTestModeEqualsTrue() throws Exception {
    assertTrue(Configuration.getBoolean(PropertyKey.TEST_MODE));
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
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      Configuration.init();
      assertEquals(PropertyKey.LOGGER_TYPE.getDefaultValue(),
          Configuration.get(PropertyKey.LOGGER_TYPE));
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
      Configuration.init();
      assertEquals("TEST_LOGGER", Configuration.get(PropertyKey.LOGGER_TYPE));
    }
  }

  @Test
  public void setIgnoredPropertiesInSiteProperties() throws Exception {
    // Need to initialize the configuration instance first, other wise in after
    // ConfigurationTestUtils.resetConfiguration() will fail due to failed class init.
    Configuration.init();
    Properties siteProps = new Properties();
    siteProps.setProperty(PropertyKey.LOGS_DIR.toString(), "/tmp/logs1");
    File propsFile = mFolder.newFile(Constants.SITE_PROPERTIES);
    siteProps.store(new FileOutputStream(propsFile), "tmp site properties file");
    Map<String, String> sysProps = new HashMap<>();
    sysProps.put(PropertyKey.SITE_CONF_DIR.toString(), mFolder.getRoot().getAbsolutePath());
    sysProps.put(PropertyKey.TEST_MODE.toString(), "false");
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      mThrown.expect(IllegalStateException.class);
      Configuration.init();
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
      Configuration.init();
      assertEquals(
          Source.SYSTEM_PROPERTY, Configuration.getSource(PropertyKey.LOGS_DIR));
      assertEquals("/tmp/logs1", Configuration.get(PropertyKey.LOGS_DIR));
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
      Configuration.init();
      assertEquals("host-1", Configuration.get(PropertyKey.MASTER_HOSTNAME));
      assertEquals("123", Configuration.get(PropertyKey.WEB_THREADS));
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
      Configuration.init();
      // set only in site prop
      assertEquals(Source.SITE_PROPERTY,
          Configuration.getSource(PropertyKey.MASTER_HOSTNAME));
      // set both in site and system prop
      assertEquals(Source.SYSTEM_PROPERTY,
          Configuration.getSource(PropertyKey.MASTER_WEB_PORT));
      // set only in system prop
      assertEquals(Source.SYSTEM_PROPERTY,
          Configuration.getSource(PropertyKey.LOGS_DIR));
      // set neither in system prop
      assertEquals(Source.DEFAULT,
          Configuration.getSource(PropertyKey.MASTER_RPC_PORT));
    }
  }

  @Test
  public void getRuntimeDefault() throws Exception {
    AtomicInteger x = new AtomicInteger(100);
    PropertyKey key = new PropertyKey.Builder("testKey")
        .setDefaultSupplier(new DefaultSupplier(() -> x.get(), "finds x"))
        .build();
    assertEquals(100, Configuration.getInt(key));
    x.set(20);
    assertEquals(20, Configuration.getInt(key));
  }

  @Test
  public void toMap() throws Exception {
    // Create a nested property to test
    String testKeyName = "alluxio.extensions.dir";
    PropertyKey nestedKey = PropertyKey.SECURITY_LOGIN_USERNAME;
    String nestedValue = String.format("${%s}.test", testKeyName);
    Configuration.set(nestedKey, nestedValue);

    Map<String, String> resolvedMap = Configuration.toMap();

    // Test if the value of the created nested property is correct
    assertEquals(Configuration.get(PropertyKey.fromString(testKeyName)),
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
    assertTrue(resolvedMap.containsKey("alluxio.logserver.logs.dir"));
    assertTrue(resolvedMap.containsKey("alluxio.master.journal.folder"));
    assertTrue(resolvedMap.containsKey("alluxio.proxy.web.port"));
    assertTrue(resolvedMap.containsKey("alluxio.security.authentication.type"));
    assertTrue(resolvedMap.containsKey("alluxio.user.block.master.client.threads"));
    assertTrue(resolvedMap.containsKey("alluxio.worker.bind.host"));
  }

  @Test
  public void toRawMap() throws Exception {
    // Create a nested property to test
    PropertyKey testKey = PropertyKey.SECURITY_LOGIN_USERNAME;
    String testValue = String.format("${%s}.test", "alluxio.extensions.dir");
    Configuration.set(testKey, testValue);

    Map<String, String> rawMap = Configuration.toRawMap();

    // Test if the value of the created nested property remains raw
    assertEquals(testValue, rawMap.get(testKey.toString()));

    // Test if some values in raw map is of ${VALUE} format
    String regexString = "(\\$\\{([^{}]*)\\})";
    Pattern confRegex = Pattern.compile(regexString);
    assertTrue(confRegex.matcher(rawMap.get("alluxio.locality.script")).find());
    assertTrue(confRegex.matcher(rawMap.get("alluxio.logs.dir")).find());
  }
}
