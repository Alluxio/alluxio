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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Common util methods for executing commands.
 */
@ThreadSafe
public final class CommandUtils {

  private CommandUtils() {
    // Not intended for instantiation.
  }

  /**
   * Sets a new TTL value or unsets an existing TTL value for file at path.
   *
   * @param fs the file system for Alluxio
   * @param path the file path
   * @param ttlMs the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned; {@link Constants#NO_TTL} means to unset the TTL value
   * @throws IOException when failing to set/unset the TTL
   */
  public static void setTtl(FileSystem fs, AlluxioURI path, long ttlMs) throws IOException {
    try {
      SetAttributeOptions options = SetAttributeOptions.defaults().setTtl(ttlMs);
      fs.setAttribute(path, options);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Converts a millisecond number to a formatted date String.
   *
   * @param millis a long millisecond number
   * @return formatted date String
   */
  public static String convertMsToDate(long millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(millis));
  }

  /**
   * Sets pin state for the input path.
   *
   * @param fs The {@link FileSystem} client
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param pinned the state to be set
   * @throws IOException if a non-Alluxio related exception occurs
   */
  public static void setPinned(FileSystem fs, AlluxioURI path, boolean pinned)
      throws IOException {
    try {
      SetAttributeOptions options = SetAttributeOptions.defaults().setPinned(pinned);
      fs.setAttribute(path, options);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }
}
