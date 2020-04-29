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

package alluxio.jnifuse.utils;

import alluxio.jnifuse.FuseException;

import java.io.IOException;
import java.nio.file.Path;

public class MountUtils {
  /**
   * Perform/force a umount at the provided Path
   */
  public static void umount(Path mountPoint) {
    String mountPath = mountPoint.toAbsolutePath().toString();
    try {
      new ProcessBuilder("fusermount", "-u", "-z", mountPath).start();
    } catch (IOException e) {
      try {
        new ProcessBuilder("umount", mountPath).start().waitFor();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new FuseException("Unable to umount FS", e);
      } catch (IOException ioe) {
        ioe.addSuppressed(e);
        throw new FuseException("Unable to umount FS", ioe);
      }
    }
  }
}
