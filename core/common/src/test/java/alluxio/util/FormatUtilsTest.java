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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    List<TestCase> testCases = new ArrayList<>();
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
      assertEquals(testCase.mExpected, FormatUtils.parametersToString(testCase.mInput));
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

    List<TestCase> testCases = new ArrayList<>();
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
      assertEquals(testCase.mExpected, FormatUtils.byteBufferToString(testCase.mInput));
    }
  }

  /**
   * Tests the {@link FormatUtils#byteArrayToHexString(byte[])} method.
   */
  @Test
  public void byteArrayToHexString() {
    assertEquals("", FormatUtils.byteArrayToHexString(new byte[0]));
    assertEquals("0x01", FormatUtils.byteArrayToHexString(new byte[]{1}));
    assertEquals("0x01 0xac", FormatUtils.byteArrayToHexString(new byte[]{1, (byte) 0xac}));
    assertEquals("01ac",
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

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase("^Task A took (.*) ms.$", "Task A"));
    testCases.add(new TestCase("^Task B took (.*) ms.$", "Task B"));

    long delta = 100;
    for (TestCase testCase : testCases) {
      String result =
          FormatUtils.formatTimeTakenMs(CommonUtils.getCurrentMs() - delta, testCase.mInputMessage);
      Matcher match = testCase.mExpected.matcher(result);
      assertTrue(match.matches());
      assertTrue(delta <= Long.parseLong(match.group(1)));
      assertTrue(Long.parseLong(match.group(1)) <= 2 * delta);
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

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase("^Task A took (.*) ns.$", "Task A"));
    testCases.add(new TestCase("^Task B took (.*) ns.$", "Task B"));

    long delta = 100000000;
    for (TestCase testCase : testCases) {
      String result =
          FormatUtils.formatTimeTakenNs(System.nanoTime() - delta, testCase.mInputMessage);
      Matcher match = testCase.mExpected.matcher(result);
      assertTrue(match.matches());
      assertTrue(delta <= Long.parseLong(match.group(1)));
      assertTrue(Long.parseLong(match.group(1)) <= 2 * delta);
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

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase("4B", 1L << 2));
    testCases.add(new TestCase("8B", 1L << 3));
    testCases.add(new TestCase("4096B", 1L << 12));
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
      assertEquals(testCase.mExpected, FormatUtils.getSizeFromBytes(testCase.mInput));
    }
  }

  /**
   * Tests the {@link FormatUtils#parseSpaceSize(String)} method.
   */
  @Test
  public void parseSpaceSize() {
    long max = 10240;
    for (long k = 0; k < max; k++) {
      assertEquals(k / 10, FormatUtils.parseSpaceSize(k / 10.0 + "b"));
      assertEquals(k / 10, FormatUtils.parseSpaceSize(k / 10.0 + "B"));
      assertEquals(k / 10, FormatUtils.parseSpaceSize(k / 10.0 + ""));
    }
    for (long k = 0; k < max; k++) {
      assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "kb"));
      assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Kb"));
      assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "KB"));
      assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "kB"));
      assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "k"));
      assertEquals(k * Constants.KB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "K"));
    }
    for (long k = 0; k < max; k++) {
      assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "mb"));
      assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Mb"));
      assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "MB"));
      assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "mB"));
      assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "m"));
      assertEquals(k * Constants.MB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "M"));
    }
    for (long k = 0; k < max; k++) {
      assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "gb"));
      assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Gb"));
      assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "GB"));
      assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "gB"));
      assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "g"));
      assertEquals(k * Constants.GB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "G"));
    }
    for (long k = 0; k < max; k++) {
      assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "tb"));
      assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Tb"));
      assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "TB"));
      assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "tB"));
      assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "t"));
      assertEquals(k * Constants.TB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "T"));
    }
    // We stop the pb test before 8192, since 8192 petabytes is beyond the scope of a java long.
    for (long k = 0; k < 8192; k++) {
      assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "pb"));
      assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "Pb"));
      assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "PB"));
      assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "pB"));
      assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "p"));
      assertEquals(k * Constants.PB / 10, FormatUtils.parseSpaceSize(k / 10.0 + "P"));
    }
  }

  /**
   * Tests the {@link FormatUtils#formatMode(short, boolean, boolean)} method.
   */
  @Test
  public void formatPermission() {
    assertEquals("-rw-rw-rw-", FormatUtils.formatMode((short) 0666, false, false));
    assertEquals("drw-rw-rw-", FormatUtils.formatMode((short) 0666, true, false));
    assertEquals("-rwxrwxrwx", FormatUtils.formatMode((short) 0777, false, false));
    assertEquals("drwxrwxrwx", FormatUtils.formatMode((short) 0777, true, false));
    assertEquals("-r--r--r--", FormatUtils.formatMode((short) 0444, false, false));
    assertEquals("dr--r--r--", FormatUtils.formatMode((short) 0444, true, false));
    assertEquals("-r-xr-xr-x", FormatUtils.formatMode((short) 0555, false, false));
    assertEquals("dr-xr-xr-x", FormatUtils.formatMode((short) 0555, true, false));
    assertEquals("-rwxr-xr--", FormatUtils.formatMode((short) 0754, false, false));
    assertEquals("drwxr-xr--", FormatUtils.formatMode((short) 0754, true, false));
  }

  @Test
  public void formatPermissionExtendedAcl() {
    assertEquals("-rw-rw-rw-+", FormatUtils.formatMode((short) 0666, false, true));
    assertEquals("drw-rw-rw-+", FormatUtils.formatMode((short) 0666, true, true));
    assertEquals("-rwxrwxrwx+", FormatUtils.formatMode((short) 0777, false, true));
    assertEquals("drwxrwxrwx+", FormatUtils.formatMode((short) 0777, true, true));
  }
}
