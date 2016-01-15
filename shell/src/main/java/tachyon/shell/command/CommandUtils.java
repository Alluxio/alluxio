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

package tachyon.shell.command;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.SetStateOptions;
import tachyon.exception.TachyonException;

/**
 * Common util methods for executing commands.
 */
public final class CommandUtils {

  private CommandUtils() {
    // Not intended for instantiation.
  }

  /**
   * Sets a new TTL value or unsets an existing TTL value for file at path.
   *
   * @param path the file path
   * @param ttlMs the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned; {@link Constants#NO_TTL} means to unset the TTL value
   * @throws IOException when failing to set/unset the TTL
   */
  public static void setTtl(TachyonFileSystem tfs, TachyonURI path, long ttlMs) throws IOException {
    try {
      TachyonFile fd = tfs.open(path);
      SetStateOptions options = new SetStateOptions.Builder().setTtl(ttlMs).build();
      tfs.setState(fd, options);
    } catch (TachyonException e) {
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
   * Converts an int permission value to a formatted String.
   *
   * @param permission value of permission for the path
   * @param isDir whether the path is a directory
   * @return formatted permission String
   */
  public static String formatPermission(int permission, boolean isDir) {
    StringBuilder permString = new StringBuilder();

    for (int i = 0; i < 3; i ++) {
      if ((permission & 0x01) == 0x01) {
        permString.append("x");
      } else {
        permString.append("-");
      }
      if ((permission & 0x02) == 0x02) {
        permString.append("w");
      } else {
        permString.append("-");
      }
      if ((permission & 0x04) == 0x04) {
        permString.append("r");
      } else {
        permString.append("-");
      }
      permission >>= 3;
    }
    if (isDir) {
      permString.append("d");
    } else {
      permString.append("-");
    }
    return permString.reverse().toString();
  }

  /**
   * Sets pin state for the input path
   *
   * @param tfs The {@link TachyonFileSystem} client
   * @param path The {@link TachyonURI} path as the input of the command
   * @param pinned the state to be set
   * @throws IOException if a non-Tachyon related exception occurs
   */
  public static void setPinned(TachyonFileSystem tfs, TachyonURI path, boolean pinned)
      throws IOException {
    try {
      TachyonFile fd = tfs.open(path);
      SetStateOptions options = new SetStateOptions.Builder().setPinned(pinned).build();
      tfs.setState(fd, options);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }
}
