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

import static jnr.constants.platform.OpenFlags.*;

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
    // for alluxio fuse, can support O_RDONLY or O_WRONLY, for O_RDWR we defer to first read or write
    switch (valueOf(flag & O_ACCMODE.intValue())) {
      case O_RDONLY:
        return OpenAction.READ_ONLY;
      case O_WRONLY:
        return OpenAction.WRITE_ONLY;
      case O_RDWR:
        return OpenAction.READ_WRITE;
      default:
        return OpenAction.NOT_SUPPORTED;
    }
  }

  /**
   * Alluxio Fuse open action.
   * Defines what operation Alluxio should perform in the Fuse.open().
   */
  public enum OpenAction {
    READ_ONLY,
    WRITE_ONLY,
    // TODO(maobaolong): Add an option to decide whether reject rw flag
    // Alluxio does not support open a file for reading and writing concurrently.
    // Read or write behavior is decided in the first read() or write()
    // if read then write or write then read, we error out
    READ_WRITE,
    NOT_SUPPORTED,
    UNKNOWN,
    ;
  }
}
