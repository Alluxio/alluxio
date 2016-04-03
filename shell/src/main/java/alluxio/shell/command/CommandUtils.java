/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
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
