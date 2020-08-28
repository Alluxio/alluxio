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
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.master.journal.JournalType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public final class MultiProcessClusterTest {
  private MultiProcessCluster mCluster;

  @Rule
  public Timeout mTimeout = Timeout.millis(Constants.MAX_TEST_DURATION_MS);

  @Test
  public void simpleCluster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.MULTI_PROCESS_SIMPLE_CLUSTER)
        .setClusterName("simpleCluster")
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    clusterVerification();
  }

  @Test
  public void zookeeper() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.MULTI_PROCESS_ZOOKEEPER)
        .setClusterName("zookeeper")
        .setNumMasters(3)
        .setNumWorkers(2)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .build();
    clusterVerification();
  }

  /**
   * Verifies cluster running as expected.
   */
  private void clusterVerification() throws Exception {
    try {
      mCluster.start();
      mCluster.waitForAllNodesRegistered(60 * Constants.SECOND_MS);
      FileSystem fs = mCluster.getFileSystemClient();
      createAndOpenFile(fs);
      mCluster.notifySuccess();
    } finally {
      mCluster.destroy();
    }
  }

  private static void createAndOpenFile(FileSystem fs) throws Exception {
    long timeoutMs = System.currentTimeMillis() + Constants.MINUTE_MS;
    int len = 10;
    for (;;) {
      String testFile = "/fileName";
      try {
        FileSystemTestUtils.createByteFile(fs, testFile, len,
            CreateFilePOptions.newBuilder().setBlockSizeBytes(100).build());
        break;
      } catch (Exception e) {
        // This can indicate that the worker hasn't connected yet, so we must delete and retry.
        if (System.currentTimeMillis() < timeoutMs) {
          try {
            if (fs.exists(new AlluxioURI(testFile))) {
              fs.delete(new AlluxioURI(testFile));
            }
          } catch (Exception ee) {
            // the cleanup can still fail because master is not ready. simply ignore the exception.
          }
        } else {
          fail(String.format("Timed out trying to create a file. Latest exception: %s",
              e.toString()));
        }
      }
    }
    try (FileInStream is = fs.openFile(new AlluxioURI("/fileName"))) {
      assertEquals(10, is.remaining());
    }
  }
}
