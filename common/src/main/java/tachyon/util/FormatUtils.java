/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.util;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.slf4j.Logger;

import tachyon.Constants;

/**
 * Utility methods to parse specific formats, print according to specific formats or transform among
 * different formats. They are not only bound to string format but also include parsing objects to a
 * specific string format, parsing a specific string format back to objects and printing or logging
 * according to a specific format.
 */
public class FormatUtils {
  /**
   * Parse a list of <code>Objects</code> into a <code>String</code>.
   *
   * @param objs a list of Objects to convert to a String
   * @return comma-separated concatenation of the string representation returned by Object#toString
   *         of the individual objects
   */
  public static String parametersToString(Object... objs) {
    StringBuilder sb = new StringBuilder("(");
    for (int k = 0; k < objs.length; k ++) {
      if (k != 0) {
        sb.append(", ");
      }
      sb.append(objs[k].toString());
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Print the given buffer. In particular, the function prints the content of the buffer in 4-byte
   * increments as space separated integers.
   *
   * @param logger logger to use
   * @param buf buffer to use
   */
  public static void printByteBuffer(Logger logger, ByteBuffer buf) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < buf.limit() / 4; k ++) {
      sb.append(buf.getInt()).append(" ");
    }

    logger.info(sb.toString());
  }

  /**
   * Print time elapsed since the given start time.
   *
   * @param startTimeMs start time in milliseconds
   * @param logger logger to use
   * @param message prefix for the message to be printed
   */
  public static void printTimeTakenMs(long startTimeMs, Logger logger, String message) {
    logger.info(message + " took " + (CommonUtils.getCurrentMs() - startTimeMs) + " ms.");
  }

  /**
   * Print time elapsed since the given start time.
   *
   * @param startTimeNs start time in nanoseconds
   * @param logger logger to use
   * @param message prefix for the message to be printed
   */
  public static void printTimeTakenNs(long startTimeNs, Logger logger, String message) {
    logger.info(message + " took " + (System.nanoTime() - startTimeNs) + " ns.");
  }

  /**
   * Return a human-readable version of bytes 10GB 2048KB etc.
   *
   * @param bytes the number of bytes
   * @return human readable version
   */
  public static String getSizeFromBytes(long bytes) {
    double ret = bytes;
    if (ret <= 1024 * 5) {
      return String.format("%.2f B", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f KB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f MB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f GB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f TB", ret);
    }
    return String.format("%.2f PB", ret);
  }

  /**
   * Parse a String size to Bytes.
   *
   * @param spaceSize the size of a space, e.g. 10GB, 5TB, 1024
   * @return the space size in bytes
   */
  public static long parseSpaceSize(String spaceSize) {
    double alpha = 0.0001;
    String ori = spaceSize;
    String end = "";
    int tIndex = spaceSize.length() - 1;
    while (tIndex >= 0) {
      if (spaceSize.charAt(tIndex) > '9' || spaceSize.charAt(tIndex) < '0') {
        end = spaceSize.charAt(tIndex) + end;
      } else {
        break;
      }
      tIndex --;
    }
    spaceSize = spaceSize.substring(0, tIndex + 1);
    double ret = Double.parseDouble(spaceSize);
    end = end.toLowerCase();
    if (end.isEmpty() || end.equals("b")) {
      return (long) (ret + alpha);
    } else if (end.equals("kb")) {
      return (long) (ret * Constants.KB + alpha);
    } else if (end.equals("mb")) {
      return (long) (ret * Constants.MB + alpha);
    } else if (end.equals("gb")) {
      return (long) (ret * Constants.GB + alpha);
    } else if (end.equals("tb")) {
      return (long) (ret * Constants.TB + alpha);
    } else if (end.equals("pb")) {
      // When parsing petabyte values, we can't multiply with doubles and longs, since that will
      // lose presicion with such high numbers. Therefore we use a BigDecimal.
      BigDecimal pBDecimal = new BigDecimal(Constants.PB);
      return pBDecimal.multiply(BigDecimal.valueOf(ret)).longValue();
    } else {
      throw new IllegalArgumentException("Fail to parse " + ori + " to bytes");
    }
  }
}
