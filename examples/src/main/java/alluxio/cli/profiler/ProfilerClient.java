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

package alluxio.cli.profiler;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Profiles operations.
 */
public abstract class ProfilerClient {

  protected static final int CHUNK_SIZE = 10 * Constants.MB;
  protected static final byte[] DATA = new byte[CHUNK_SIZE];
  public static boolean sDryRun = false;

  /**
   * Create new.
   */
  public static class Factory {

    private static final AlluxioConfiguration CONF =
        new InstancedConfiguration(ConfigurationUtils.defaults());

    /**
     * Profiler client.
     * @param type alluxio or hadoop
     * @return profiler client
     */
    public static ProfilerClient create(String type) {
      switch (type) {
        case "abstractfs":
          Configuration conf = new Configuration();
          conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
          return new HadoopProfilerClient("alluxio:///", conf);
        case "alluxio":
          return new AlluxioProfilerClient();
        case "hadoop":
          return new HadoopProfilerClient(CONF.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS),
              new Configuration());
        default:
          throw new IllegalArgumentException(String.format("No profiler for %s", type));
      }
    }
  }

  protected void clientOperation(Runnable action, String msg) {
    if(!sDryRun) {
      action.run();
    } else {
      System.out.println(msg);
    }
  }

  static void writeOutput(OutputStream os, long fileSize) throws IOException {
    for (int k = 0; k < fileSize / CHUNK_SIZE; k++) {
      os.write(DATA);
    }

    os.write(Arrays.copyOf(DATA, (int)(fileSize % CHUNK_SIZE))); // Write any remaining data
  }

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @throws IOException
   */
  public abstract void cleanup(String dir) throws IOException;

  /**
   * Makes sure the path exists without any existing inodes below. Essentially cleaning up any
   * previous operations on the same location.
   *
   * @param dir directory to clean
   * @param numFiles the total number of files to create
   * @param filesPerDir number of files to create in each directory
   * @param fileSize the size in bytes of each file that will be created
   * @throws IOException
   */
  public abstract void createFiles(String dir, long numFiles, long filesPerDir, long fileSize) throws IOException;
}
