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

package alluxio.web;

import alluxio.Constants;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class WebUtils {
  /**
   * Converts a byte array to string.
   *
   * @param data byte array
   * @param offset offset
   * @param length number of bytes to encode
   * @return string representation of the encoded byte sub-array
   */
  public static String convertByteArrayToStringWithoutEscape(byte[] data, int offset, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = offset; i < length && i < data.length; i++) {
      sb.append((char) data[i]);
    }
    return sb.toString();
  }

  /**
   * Converts milliseconds to clock time.
   *
   * @param millis milliseconds
   * @return input encoded as clock time
   */
  public static String convertMsToClockTime(long millis) {
    Preconditions.checkArgument(millis >= 0, "Negative values are not supported");

    long days = millis / Constants.DAY_MS;
    long hours = (millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d day(s), %d hour(s), %d minute(s), and %d second(s)", days, hours,
        mins, secs);
  }

  /**
   * Converts milliseconds to short clock time.
   *
   * @param millis milliseconds
   * @return input encoded as short clock time
   */
  public static String convertMsToShortClockTime(long millis) {
    Preconditions.checkArgument(millis >= 0, "Negative values are not supported");

    long days = millis / Constants.DAY_MS;
    long hours = (millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d d, %d h, %d m, and %d s", days, hours, mins, secs);
  }

  private WebUtils() {} // prevent instantiation
}
