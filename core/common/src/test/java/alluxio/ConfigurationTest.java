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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for the {@link Configuration} class.
 */
public class ConfigurationTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void defaultHomeCorrectlyLoaded() {
    String alluxioHome = Configuration.get(Constants.HOME);
    Assert.assertEquals("/mnt/alluxio_default_home", alluxioHome);
  }

  @Test
  public void getInt() {
    Configuration.set("key", "1");
    Assert.assertEquals(1, Configuration.getInt("key"));
  }

  @Test
  public void getMalformedIntThrowsException() {
    Configuration.set("key", "9448367483758473854738"); // bigger than MAX_INT
    mThrown.expect(RuntimeException.class);
    Configuration.getInt("key");
  }

  @Test
  public void getLong() {
    Configuration.set("key", "12345678910"); // bigger than MAX_INT
    Assert.assertEquals(12345678910L, Configuration.getLong("key"));
  }

  @Test
  public void getMalformedLongThrowsException() {
    Configuration.set("key", "999999999999999999999999999999999999"); // bigger than MAX_LONG
    mThrown.expect(RuntimeException.class);
    Configuration.getLong("key");
  }

  @Test
  public void getDouble() {
    Configuration.set("key", "1.1");
    Assert.assertEquals(1.1, Configuration.getDouble("key"), /*tolerance=*/0.0001);
  }

  @Test
  public void getMalformedDoubleThrowsException() {
    Configuration.set("key", "1a");
    mThrown.expect(RuntimeException.class);
    Configuration.getDouble("key");
  }

  @Test
  public void getFloat() {
    Configuration.set("key", "1.1");
    Assert.assertEquals(1.1, Configuration.getFloat("key"), /*tolerance=*/0.0001);
  }

  @Test
  public void getMalformedFloatThrowsException() {
    Configuration.set("key", "1a");
    mThrown.expect(RuntimeException.class);
    Configuration.getFloat("key");
  }

  @Test
  public void getTrueBoolean() {
    Configuration.set("key", "true");
    Assert.assertTrue(Configuration.getBoolean("key"));
  }

  @Test
  public void getTrueBooleanUppercase() {
    Configuration.set("key", "True");
    Assert.assertTrue(Configuration.getBoolean("key"));
  }

  @Test
  public void getTrueBooleanMixcase() {
    Configuration.set("key", "tRuE");
    Assert.assertTrue(Configuration.getBoolean("key"));
  }

  @Test
  public void getFalseBoolean() {
    Configuration.set("key", "false");
    Assert.assertFalse(Configuration.getBoolean("key"));
  }

  @Test
  public void getFalseBooleanUppercase() {
    Configuration.set("key", "False");
    Assert.assertFalse(Configuration.getBoolean("key"));
  }

  @Test
  public void getFalseBooleanMixcase() {
    Configuration.set("key", "fAlSe");
    Assert.assertFalse(Configuration.getBoolean("key"));
  }

  @Test
  public void getMalformedBooleanThrowsException() {
    Configuration.set("key", "x");
    mThrown.expect(RuntimeException.class);
    Configuration.getBoolean("key");
  }

  @Test
  public void getList() {
    Configuration.set("key", "a,b,c");
    Assert.assertEquals(Lists.newArrayList("a", "b", "c"), Configuration.getList("key", ","));
  }

  private static enum TestEnum {
    VALUE
  }

  @Test
  public void getEnum() {
    Configuration.set("key", "VALUE");
    Assert.assertEquals(TestEnum.VALUE, Configuration.getEnum("key", TestEnum.class));
  }

  @Test
  public void getMalformedEnum() {
    Configuration.set("key", "not_a_value");
    mThrown.expect(RuntimeException.class);
    Configuration.getEnum("key", TestEnum.class);
  }

  @Test
  public void getBytes() {
    Configuration.set("key", "10b");
    Assert.assertEquals(10, Configuration.getBytes("key"));
  }

  @Test
  public void getBytesKb() {
    Configuration.set("key", "10kb");
    Assert.assertEquals(10 * Constants.KB, Configuration.getBytes("key"));
  }

  @Test
  public void getBytesMb() {
    Configuration.set("key", "10mb");
    Assert.assertEquals(10 * Constants.MB, Configuration.getBytes("key"));
  }

  @Test
  public void getBytesGb() {
    Configuration.set("key", "10gb");
    Assert.assertEquals(10 * (long) Constants.GB, Configuration.getBytes("key"));
  }

  @Test
  public void getBytesGbUppercase() {
    Configuration.set("key", "10GB");
    Assert.assertEquals(10 * (long) Constants.GB, Configuration.getBytes("key"));
  }

  @Test
  public void getBytesTb() {
    Configuration.set("key", "10tb");
    Assert.assertEquals(10 * Constants.TB, Configuration.getBytes("key"));
  }

  @Test
  public void getBytespT() {
    Configuration.set("key", "10pb");
    Assert.assertEquals(10 * Constants.PB, Configuration.getBytes("key"));
  }

  @Test
  public void getMalformedBytesThrowsException() {
    Configuration.set("key", "100a");
    mThrown.expect(RuntimeException.class);
    Configuration.getBoolean("key");
  }

  @Test
  public void getClassTest() { // The name getClass is already reserved.
    Configuration.set("key", "java.lang.String");
    Assert.assertEquals(String.class, Configuration.getClass("key"));
  }

  @Test
  public void getMalformedClassThrowsException() {
    Configuration.set("key", "java.util.not.a.class");
    mThrown.expect(RuntimeException.class);
    Configuration.getClass("key");
  }

  @Test
  public void variableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        "key", "value",
        "substitution", "${key}"));
    String substitution = Configuration.get("substitution");
    Assert.assertEquals("value", substitution);
  }

  @Test
  public void twoVariableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        "key1", "value1",
        "key2", "value2",
        "substitution", "${key1},${key2}"));
    String substitution = Configuration.get("substitution");
    Assert.assertEquals("value1,value2", substitution);
  }

  @Test
  public void recursiveVariableSubstitution() {
    Configuration.merge(ImmutableMap.of(
        "key", "value",
        "substitution1", "${key}",
        "substitution2", "${substitution1}"));
    String substitution2 = Configuration.get("substitution2");
    Assert.assertEquals("value", substitution2);
  }

  @Test
  public void systemVariableSubstitution() throws Exception {
    try (SetAndRestoreSystemProperty c =
        new SetAndRestoreSystemProperty(Constants.MASTER_HOSTNAME, "new_master")) {
      Configuration.defaultInit();
      Assert.assertEquals("new_master", Configuration.get(Constants.MASTER_HOSTNAME));
    }
  }

  @Test
  public void userFileBufferBytesOverFlowException() {
    mThrown.expect(IllegalArgumentException.class);
    Configuration.set(Constants.USER_FILE_BUFFER_BYTES,
        String.valueOf(Integer.MAX_VALUE + 1) + "B");
  }

  @Test
  public void setUserFileBufferBytesMaxInteger() {
    Configuration.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(Integer.MAX_VALUE) + "B");
    Assert.assertEquals(Integer.MAX_VALUE,
        (int) Configuration.getBytes(Constants.USER_FILE_BUFFER_BYTES));
  }

  @Test
  public void setUserFileBufferBytes1GB() {
    Configuration.set(Constants.USER_FILE_BUFFER_BYTES, "1GB");
    Assert.assertEquals(1073741824, (int) Configuration.getBytes(Constants.USER_FILE_BUFFER_BYTES));
  }
}
