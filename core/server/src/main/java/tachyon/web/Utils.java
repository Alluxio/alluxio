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

package tachyon.web;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.common.base.Preconditions;

import tachyon.Constants;

final class Utils {

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
    for (int i = offset; i < length && i < data.length; i ++) {
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
   * Converts milliseconds to date.
   *
   * @param millis milliseconds
   * @return input encoded as date
   */
  public static String convertMsToDate(long millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(millis));
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

  private Utils() {} // prevent instantiation
}
