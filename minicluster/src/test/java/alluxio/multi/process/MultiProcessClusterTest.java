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

package alluxio.multi.process;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.CreateFileOptions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public final class MultiProcessClusterTest {

  @Rule
  public Timeout mTimeout = Timeout.seconds(600);

  @Test
  public void simpleCluster() throws Exception {
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    try {
      cluster.start();
      FileSystem fs = cluster.getFileSystemClient();
      createAndOpenFile(fs);
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void zookeeper() throws Exception {
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .addProperty(PropertyKey.ZOOKEEPER_ENABLED, "true")
        .setNumMasters(3)
        .setNumWorkers(2)
        .build();
    try {
      cluster.start();
      FileSystem fs = cluster.getFileSystemClient();
      createAndOpenFile(fs);
    } finally {
      cluster.destroy();
    }
  }

  private static void createAndOpenFile(FileSystem fs) throws Exception {
    int len = 10;
    for (;;) {
      try {
        FileSystemTestUtils.createByteFile(fs, "/fileName", len,
            CreateFileOptions.defaults().setBlockSizeBytes(100));
        break;
      } catch (Exception e) {
        // This can indicate that the worker hasn't connected yet, so we must retry.
      }
    }
    try (FileInStream is = fs.openFile(new AlluxioURI("/fileName"))) {
      assertEquals(10, is.remaining());
    }
  }
}
