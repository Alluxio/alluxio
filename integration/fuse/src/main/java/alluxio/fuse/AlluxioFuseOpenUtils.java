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

package alluxio.fuse;

import static jnr.constants.platform.OpenFlags.O_ACCMODE;
import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_RDWR;
import static jnr.constants.platform.OpenFlags.O_WRONLY;

import jnr.constants.platform.OpenFlags;

/**
 * Alluxio Fuse utilities to handle different fuse.open() flags.
 * Decide Alluxio open action based on fuse open flags.
 */
public final class AlluxioFuseOpenUtils {

  /**
   * Gets Alluxio Fuse open action based on open flag.
   *
   * @param flag the open flag
   * @return the open action
   */
  public static OpenAction getOpenAction(int flag) {
    // open flags must contain one of O_RDONLY(0), O_WRONLY(1), O_RDWR(2)
    // O_ACCMODE is mask of read write(3)
    // Alluxio fuse only supports read-only for completed file
    // and write-only for file that does not exist or contains open flag O_TRUNC
    // O_RDWR will be treated as read-only if file exists and no O_TRUNC,
    // write-only otherwise
    switch (OpenFlags.valueOf(flag & O_ACCMODE.intValue())) {
      case O_RDONLY:
        return OpenAction.READ_ONLY;
      case O_WRONLY:
        return OpenAction.WRITE_ONLY;
      case O_RDWR:
        return OpenAction.READ_WRITE;
      default:
        // Should not fall here
        return OpenAction.NOT_SUPPORTED;
    }
  }

  /**
   * Checks if the open flag contains truncate open flag.
   *
   * @param flag the open flag to check
   * @return true if contains truncate open flag, false otherwise
   */
  public static boolean containsTruncate(int flag) {
    return (flag & OpenFlags.O_TRUNC.intValue()) != 0;
  }

  /**
   * Checks if the open flag contains create open flag.
   *
   * @param flag the open flag to check
   * @return true if contains create open flag, false otherwise
   */
  public static boolean containsCreate(int flag) {
    return (flag & OpenFlags.O_CREAT.intValue()) != 0;
  }

  /**
   * Alluxio Fuse open action.
   * Defines what operation Alluxio should perform in the Fuse.open().
   */
  public enum OpenAction {
    READ_ONLY,
    WRITE_ONLY,
    // If file exists and no truncate flag is provided, treat as READ_ONLY,
    // otherwise treat as WRITE_ONLY
    READ_WRITE,
    // Should not fall here
    NOT_SUPPORTED,
    ;
  }
}
