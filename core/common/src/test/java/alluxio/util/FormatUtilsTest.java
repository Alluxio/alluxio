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

package alluxio.util;

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests the {@link FormatUtils} class.
 */
public final class FormatUtilsTest {

  /**
   * Tests the {@link FormatUtils#parametersToString(Object...)} method.
   */
  @Test
  public void parametersToString() {
    class TestCase {
      String mExpected;
      Object[] mInput;

      public TestCase(String expected, Object[] objs) {
        mExpected = expected;
        mInput = objs;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase("()", null));
    testCases.add(new TestCase("(null)", new Object[] {null}));
    testCases.add(new TestCase("()", new Object[] {""}));
    testCases.add(new TestCase("(foo)", new Object[] {"foo"}));
    testCases.add(new TestCase("(foo, bar)", new Object[] {"foo", "bar"}));
    testCases.add(new TestCase("(foo, , bar)", new Object[] {"foo", "", "bar"}));
    testCases.add(new TestCase("(, foo, )", new Object[] {"", "foo", ""}));
    testCases.add(new TestCase("(, , )", new Object[] {"", "", ""}));
    testCases.add(new TestCase("(1)", new Object[] {1}));
    testCases.add(new TestCase("(1, 2, 3)", new Object[] {1, 2, 3}));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(testCase.mExpected, FormatUtils.parametersToString(testCase.mInput));
    }
  }

  /**
   * Tests the {@link FormatUtils#byteBufferToString(ByteBuffer)} method.
   */
  @Test
  public void byteBufferToString() {
    class TestCase {
      String mExpected;
      ByteBuffer mInput;

      public TestCase(String expected, ByteBuffer input) {
        mExpected = expected;
        mInput = input;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase("", ByteBuffer.wrap(new byte[] {})));
    testCases.add(new TestCase("", ByteBuffer.wrap(new byte[] {0})));
    testCases.add(new TestCase("", ByteBuffer.wrap(new byte[] {0, 0})));
    testCases.add(new TestCase("", ByteBuffer.wrap(new byte[] {0, 0, 0})));
    testCases.add(new TestCase("1", ByteBuffer.wrap(new byte[] {0, 0, 0, 1})));
    testCases.add(new TestCase("1", ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0})));
    testCases.add(new TestCase("1", ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0, 0})));
    testCases.add(new TestCase("1", ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0, 0, 0})));
    testCases.add(new TestCase("1 2", ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0, 0, 0, 2})));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(testCase.mExpected, FormatUtils.byteBufferToString(testCase.mInput));
    }
  }

  /**
   * Tests the {@link FormatUtils#byteArrayToHexString(byte[])} method.
   */
  @Test
  public void byteArrayToHexString() {
    Assert.assertEquals("", FormatUtils.byteArrayToHexString(new byte[0]));
    Assert.assertEquals("0x01", FormatUtils.byteArrayToHexString(new byte[]{1}));
    Assert.assertEquals("0x01 0xac", FormatUtils.byteArrayToHexString(new byte[]{1, (byte) 0xac}));
    Assert.assertEquals("01ac",
        FormatUtils.byteArrayToHexString(new byte[] {1, (byte) 0xac}, "", ""));
  }

  /**
   * Tests the {@link FormatUtils#formatTimeTakenMs(long, String)} method.
   */
  @Test
  public void formatTimeTakenMs() {
    class TestCase {
      Pattern mExpected;
      String mInputMessage;

      public TestCase(String expectedRE, String inputMessage) {
        mExpected = Pattern.compile(expectedRE);
        mInputMessage = inputMessage;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase("^Task A took (.*) ms.$", "Task A"));
    testCases.add(new TestCase("^Task B took (.*) ms.$", "Task B"));

    long delta = 100;
    for (TestCase testCase : testCases) {
      String result =
          FormatUtils.formatTimeTakenMs(CommonUtils.getCurrentMs() - delta, testCase.mInputMessage);
      Matcher match = testCase.mExpected.matcher(result);
      Assert.assertTrue(match.matches());
      Assert.assertTrue(delta <= Long.parseLong(match.group(1)));
      Assert.assertTrue(Long.parseLong(match.group(1)) <= 2 * delta);
    }
  }

  /**
   * Tests the {@link FormatUtils#formatTimeTakenNs(long, String)} method.
   */
  @Test
  public void formatTimeTakenNs() {
    class TestCase {
      Pattern mExpected;
      String mInputMessage;

      public TestCase(String expectedRE, String inputMessage) {
        mExpected = Pattern.compile(expectedRE);
        mInputMessage = inputMessage;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase("^Task A took (.*) ns.$", "Task A"));
    testCases.add(new TestCase("^Task B took (.*) ns.$", "Task B"));

    long delta = 100000000;
    for (TestCase testCase : testCases) {
      String result =
          FormatUtils.formatTimeTakenNs(System.nanoTime() - delta, testCase.mInputMessage);
      Matcher match = testCase.mExpected.matcher(result);
      Assert.assertTrue(match.matches());
      Assert.assertTrue(delta <= Long.parseLong(match.group(1)));
      Assert.assertTrue(Long.parseLong(match.group(1)) <= 2 * delta);
    }
  }

  /**
   * Tests the {@link FormatUtils#getSizeFromBytes(long)} method.
   */
  @Test
  public void getSizeFromBytes() {
    class TestCase {
      String mExpected;
      long mInput;

      public TestCase(String expected, long input) {
        mExpected = expected;
        mInput = input;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase("4.00B", 1L << 2));
    testCases.add(new TestCase("8.00B", 1L << 3));
    testCases.add(new TestCase("4096.00B", 1L << 12));
    testCases.add(new TestCase("8.00KB", 1L << 13));
    testCases.add(new TestCase("4096.00KB", 1L << 22));
    testCases.add(new TestCase("8.00MB", 1L << 23));
    testCases.add(new TestCase("4096.00MB", 1L << 32));
    testCases.add(new TestCase("8.00GB", 1L << 33));
    testCases.add(new TestCase("4096.00GB", 1L << 42));
    testCases.add(new TestCase("8.00TB", 1L << 43));
    testCases.add(new TestCase("4096.00TB", 1L << 52));
    testCases.add(new TestCase("8.00PB", 1L << 53));
    testCases.add(new TestCase("4096.00PB", 1L << 62));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(testCase.mExpected, FormatUtils.getSizeFromBytes(testCase.mInput));
    }
  }

  /**
   * Tests the {@link FormatUtils#parseSpaceSize(String)} method.
   */
  @Test
  public void parseSpaceSize() {
    long max = 10240;
    for (long k = 0; k < max; k++) {
      Assert.assertEquals(k / 10, FormatUtils.parseSpaceSize(k / 10.0 + "b"));
      Assert.assertEquals(k / 10, FormatUtils.parseSpaceSize(k / 10.0 + "B"));
      Assert.assertEquals(k / 10, FormatUtils.parseSpaceSize(k / 10.0 + ""));
    }
    for (long k = 0; k < max; k++) {
      Assert.assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "kb"));
      Assert.assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Kb"));
      Assert.assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "KB"));
      Assert.assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "kB"));
      Assert.assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "k"));
      Assert.assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "K"));
    }
    for (long k = 0; k < max; k++) {
      Assert.assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "mb"));
      Assert.assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Mb"));
      Assert.assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "MB"));
      Assert.assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "mB"));
      Assert.assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "m"));
      Assert.assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "M"));
    }
    for (long k = 0; k < max; k++) {
      Assert.assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "gb"));
      Assert.assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Gb"));
      Assert.assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "GB"));
      Assert.assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "gB"));
      Assert.assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "g"));
      Assert.assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "G"));
    }
    for (long k = 0; k < max; k++) {
      Assert.assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "tb"));
      Assert.assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Tb"));
      Assert.assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "TB"));
      Assert.assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "tB"));
      Assert.assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "t"));
      Assert.assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "T"));
    }
    // We stop the pb test before 8192, since 8192 petabytes is beyond the scope of a java long.
    for (long k = 0; k < 8192; k++) {
      Assert.assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "pb"));
      Assert.assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Pb"));
      Assert.assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "PB"));
      Assert.assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "pB"));
      Assert.assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "p"));
      Assert.assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "P"));
    }
  }

  /**
   * Tests the {@link FormatUtils#formatMode(short, boolean)} method.
   */
  @Test
  public void formatPermission() {
    Assert.assertEquals("-rw-rw-rw-", FormatUtils.formatMode((short) 0666, false));
    Assert.assertEquals("drw-rw-rw-", FormatUtils.formatMode((short) 0666, true));
    Assert.assertEquals("-rwxrwxrwx", FormatUtils.formatMode((short) 0777, false));
    Assert.assertEquals("drwxrwxrwx", FormatUtils.formatMode((short) 0777, true));
    Assert.assertEquals("-r--r--r--", FormatUtils.formatMode((short) 0444, false));
    Assert.assertEquals("dr--r--r--", FormatUtils.formatMode((short) 0444, true));
    Assert.assertEquals("-r-xr-xr-x", FormatUtils.formatMode((short) 0555, false));
    Assert.assertEquals("dr-xr-xr-x", FormatUtils.formatMode((short) 0555, true));
    Assert.assertEquals("-rwxr-xr--", FormatUtils.formatMode((short) 0754, false));
    Assert.assertEquals("drwxr-xr--", FormatUtils.formatMode((short) 0754, true));
  }
}
