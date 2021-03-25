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

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Main entry for StackFS.
 */
public class StackMain {

  /**
   * @param args arguments
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: <mountPoint> <sourcePath> <fuseOpts e.g. -obig_writes...>");
      System.exit(1);
    }
    Path root = Paths.get(args[1]);
    Path mountPoint = Paths.get(args[0]);
    StackFS fs = new StackFS(root, mountPoint);
    String[] fuseOpts = new String[args.length - 2];
    System.arraycopy(args, 2, fuseOpts, 0, args.length - 2);
    try {
      fs.mount(true, false, fuseOpts);
    } catch (Exception e) {
      e.printStackTrace();
      fs.umount();
      System.exit(1);
    }
  }
}
