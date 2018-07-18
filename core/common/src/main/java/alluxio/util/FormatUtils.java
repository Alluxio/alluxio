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
import alluxio.security.authorization.Mode;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods to parse specific formats, print according to specific formats or transform among
 * different formats. They are not only bound to string format but also include parsing objects to a
 * specific string format, parsing a specific string format back to objects and printing or logging
 * according to a specific format.
 */
@ThreadSafe
public final class FormatUtils {
  /**
   * Parses a list of {@code Objects} into a {@code String}.
   *
   * @param objs a list of Objects to convert to a String
   * @return comma-separated concatenation of the string representation returned by Object#toString
   *         of the individual objects
   */
  public static String parametersToString(Object... objs) {
    StringBuilder sb = new StringBuilder("(");
    if (objs != null) {
      for (int k = 0; k < objs.length; k++) {
        if (k != 0) {
          sb.append(", ");
        }
        if (objs[k] == null) {
          sb.append("null");
        } else {
          sb.append(objs[k].toString());
        }
      }
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Parses a {@link ByteBuffer} into a {@link String}. In particular, the function prints the
   * content of the buffer in 4-byte increments as space separated integers.
   *
   * @param buf buffer to use
   * @return the String representation of the {@link ByteBuffer}
   */
  public static String byteBufferToString(ByteBuffer buf) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < buf.limit() / 4; k++) {
      if (k != 0) {
        sb.append(" ");
      }
      sb.append(buf.getInt());
    }
    return sb.toString();
  }

  /**
   * Parses a byte array into a space separated hex string where each byte is represented in the
   * format {@code 0x%02x}.
   *
   * @param bytes the byte array to be transformed
   * @return the string representation of the byte array
   */
  public static String byteArrayToHexString(byte[] bytes) {
    return byteArrayToHexString(bytes, "0x", " ");
  }

  /**
   * Parses a byte array into a hex string where each byte is represented in the
   * format {@code %02x}.
   *
   * @param bytes the byte array to be transformed
   * @param prefix the prefix to use
   * @param separator the separator to use
   * @return the string representation of the byte array
   */
  public static String byteArrayToHexString(byte[] bytes, String prefix, String separator) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(String.format("%s%02x", prefix, bytes[i]));
      if (i != bytes.length - 1) {
        sb.append(separator);
      }
    }
    return sb.toString();
  }

  /**
   * Formats time elapsed since the given start time (in milliseconds).
   *
   * @param startTimeMs start time in milliseconds
   * @param message prefix for the message to be printed
   * @return formatted string with the elapsed time (in milliseconds)
   */
  public static String formatTimeTakenMs(long startTimeMs, String message) {
    return message + " took " + (CommonUtils.getCurrentMs() - startTimeMs) + " ms.";
  }

  /**
   * Formats time elapsed since the given start time (in nanoseconds).
   *
   * @param startTimeNs start time in nanoseconds
   * @param message prefix for the message to be printed
   * @return formatted string with the elapsed time (in nanoseconds)
   */
  public static String formatTimeTakenNs(long startTimeNs, String message) {
    return message + " took " + (System.nanoTime() - startTimeNs) + " ns.";
  }

  /**
   * Returns a human-readable version of bytes 10GB 2048KB etc.
   *
   * @param bytes the number of bytes
   * @return human readable version
   */
  public static String getSizeFromBytes(long bytes) {
    double ret = bytes;
    if (ret <= 1024 * 5) {
      return String.format(Locale.ENGLISH, "%dB", bytes);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format(Locale.ENGLISH, "%.2fKB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format(Locale.ENGLISH, "%.2fMB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format(Locale.ENGLISH, "%.2fGB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format(Locale.ENGLISH, "%.2fTB", ret);
    }
    ret /= 1024;
    return String.format(Locale.ENGLISH, "%.2fPB", ret);
  }

  /**
   * Parses a String size to Bytes.
   *
   * @param spaceSize the size of a space, e.g. 10GB, 5TB, 1024
   * @return the space size in bytes
   */
  public static long parseSpaceSize(String spaceSize) {
    double alpha = 0.0001;
    String ori = spaceSize;
    String end = "";
    int index = spaceSize.length() - 1;
    while (index >= 0) {
      if (spaceSize.charAt(index) > '9' || spaceSize.charAt(index) < '0') {
        end = spaceSize.charAt(index) + end;
      } else {
        break;
      }
      index--;
    }
    spaceSize = spaceSize.substring(0, index + 1);
    double ret = Double.parseDouble(spaceSize);
    end = end.toLowerCase();
    if (end.isEmpty() || end.equals("b")) {
      return (long) (ret + alpha);
    } else if (end.equals("kb") || end.equals("k")) {
      return (long) (ret * Constants.KB + alpha);
    } else if (end.equals("mb") || end.equals("m")) {
      return (long) (ret * Constants.MB + alpha);
    } else if (end.equals("gb") || end.equals("g")) {
      return (long) (ret * Constants.GB + alpha);
    } else if (end.equals("tb") || end.equals("t")) {
      return (long) (ret * Constants.TB + alpha);
    } else if (end.equals("pb") || end.equals("p")) {
      // When parsing petabyte values, we can't multiply with doubles and longs, since that will
      // lose presicion with such high numbers. Therefore we use a BigDecimal.
      BigDecimal pBDecimal = new BigDecimal(Constants.PB);
      return pBDecimal.multiply(BigDecimal.valueOf(ret)).longValue();
    } else {
      throw new IllegalArgumentException("Fail to parse " + ori + " to bytes");
    }
  }

  /**
   * Regular expression pattern to separate digits (negative sign allowed) and letters in a string.
   */
  private static final Pattern SEP_DIGIT_LETTER = Pattern.compile("([-]?[0-9]*)([a-zA-Z]*)");

  /**
   * Parses a String size to Milliseconds. Supports negative numbers.
   *
   * @param timeSize the size of a time, e.g. 1M, 5H, 10D, -1
   * @return the time size in milliseconds
   */
  public static long parseTimeSize(String timeSize) {
    double alpha = 0.0001;
    String time = "";
    String size = "";
    Matcher m = SEP_DIGIT_LETTER.matcher(timeSize);
    if (m.matches()) {
      time = m.group(1);
      size = m.group(2);
    }
    double douTime = Double.parseDouble(time);
    long sign = 1;
    if (douTime < 0) {
      sign = -1;
      douTime = -douTime;
    }
    size = size.toLowerCase();
    if (size.isEmpty() || size.equalsIgnoreCase("ms")
        || size.equalsIgnoreCase("millisecond")) {
      return sign * (long) (douTime + alpha);
    } else if (size.equalsIgnoreCase("s") || size.equalsIgnoreCase("sec")
        || size.equalsIgnoreCase("second")) {
      return sign * (long) (douTime * Constants.SECOND + alpha);
    } else if (size.equalsIgnoreCase("m") || size.equalsIgnoreCase("min")
        || size.equalsIgnoreCase("minute")) {
      return sign * (long) (douTime * Constants.MINUTE + alpha);
    } else if (size.equalsIgnoreCase("h") || size.equalsIgnoreCase("hr")
        || size.equalsIgnoreCase("hour")) {
      return sign * (long) (douTime * Constants.HOUR + alpha);
    } else if (size.equalsIgnoreCase("d") || size.equalsIgnoreCase("day")) {
      return sign * (long) (douTime * Constants.DAY + alpha);
    } else {
      throw new IllegalArgumentException("Fail to parse " + timeSize + " to milliseconds");
    }
  }

  /**
   * Formats digital representation of a model as a human-readable string.
   *
   * @param mode file mode
   * @param directory if the mode corresponds to a directory
   * @param hasExtended true if extended acls exist
   * @return human-readable version of the given mode
   */
  public static String formatMode(short mode, boolean directory, boolean hasExtended) {
    StringBuilder str = new StringBuilder();
    if (directory) {
      str.append("d");
    } else {
      str.append("-");
    }
    str.append(new Mode(mode).toString());
    if (hasExtended) {
      str.append("+");
    }
    return str.toString();
  }

  private FormatUtils() {} // prevent instantiation
}
