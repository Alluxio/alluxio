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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

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
        PropertyKey.LOGS_DIR, "${alluxio.work.dir}/logs"));
    String substitution = Configuration.get(PropertyKey.LOGS_DIR);
    assertEquals("value/logs", substitution);
  }

  @Test
  public void twoVariableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, "value1",
        PropertyKey.MASTER_RPC_PORT, "value2",
        PropertyKey.MASTER_ADDRESS, "${alluxio.master.hostname}:${alluxio.master.port}"));
    String substitution = Configuration.get(PropertyKey.MASTER_ADDRESS);
    assertEquals("value1:value2", substitution);
  }

  @Test
  public void recursiveVariableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        PropertyKey.WORK_DIR, "value",
        PropertyKey.LOGS_DIR, "${alluxio.work.dir}/logs",
        PropertyKey.SITE_CONF_DIR, "${alluxio.logs.dir}/conf"));
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
  public void userFileBufferBytesOverFlowException() {
    mThrown.expect(IllegalStateException.class);
    Configuration.set(PropertyKey.USER_FILE_BUFFER_BYTES,
        String.valueOf(Integer.MAX_VALUE + 1) + "B");
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
  public void unsetDefaultValue() {
    assertTrue(Configuration.containsKey(PropertyKey.USER_FILE_BUFFER_BYTES));
    Configuration.unset(PropertyKey.USER_FILE_BUFFER_BYTES);
    assertFalse(Configuration.containsKey(PropertyKey.USER_FILE_BUFFER_BYTES));
  }

  @Test
  public void propertyTestModeEqualsTrue() throws Exception {
    assertTrue(Configuration.getBoolean(PropertyKey.TEST_MODE));
  }

  @Test
  public void sitePropertiesNotLoadedInTest() throws Exception {
    Properties props = new Properties();
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "TEST_LOGGER");
    File propsFile = mFolder.newFile(Configuration.SITE_PROPERTIES);
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
    File propsFile = mFolder.newFile(Configuration.SITE_PROPERTIES);
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
}
