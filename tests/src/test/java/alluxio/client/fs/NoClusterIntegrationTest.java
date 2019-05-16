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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Throwables;
import org.junit.Test;

public class NoClusterIntegrationTest {

  @Test
  public void dumbtestNoCluster() throws Exception {
    int sleepSecs = 25;

    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS, "3s");
    conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "true");
    FileSystem fs = FileSystem.Factory.create(conf);
    int count = 0;
    while (count < sleepSecs) {
      try {
        fs.openFile(new AlluxioURI(("/dir")));
      } catch (Exception e) {
        return;
      }
      Thread.sleep(1000);
      count++;
    }
    Thread.sleep(Constants.SECOND * sleepSecs);
  }

  @Test
  public void dumbtest() throws Exception {
    LocalAlluxioCluster alluxio = new LocalAlluxioCluster();
    alluxio.initConfiguration();
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, "3s");
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "2s");
    alluxio.start();
    LocalAlluxioJobCluster jobService = new LocalAlluxioJobCluster();
    jobService.start();

    int sleepSecs = 10;

    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS, "5s");
    conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "true");
    conf.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "12");
    conf.set(PropertyKey.USER_FILE_BUFFER_BYTES, "500");
    int count = 0;
    while (true) {
      try (FileSystem fs = FileSystem.Factory.create(conf)) {
        try (FileOutStream fos = fs.createFile(new AlluxioURI(String.format("/%d", count)))) {
          for (int i = 0; i < 25; i++) {
            fos.write((byte) i);
            Thread.sleep(10);
          }
        }
        count++;
      }
      alluxio.stopWorkers();
      Thread.sleep(Constants.SECOND * sleepSecs); // make the master "lose" the worker
      alluxio.startWorkers();
    }
  }

  @Test
  public void ae232Test() throws Exception {

    int workerMemoryBytes = Constants.KB;
    double watermarkHighLevel = 0.5;
    double watermarkLowLevel = 0.4;

    LocalAlluxioCluster alluxio = new LocalAlluxioCluster();
    alluxio.initConfiguration();
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, "3s");
    ServerConfiguration.set(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "2s");
    ServerConfiguration.set(PropertyKey.WORKER_MEMORY_SIZE, workerMemoryBytes);
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO,
        watermarkHighLevel);
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_LEVEL0_LOW_WATERMARK_RATIO,
        watermarkLowLevel);
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "4");
    ServerConfiguration.set(PropertyKey.USER_FILE_BUFFER_BYTES, "4");
    ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, "1s");
    alluxio.start();
    LocalAlluxioJobCluster jobService = new LocalAlluxioJobCluster();
    jobService.start();

    FileSystem fs = FileSystem.Factory.create(ServerConfiguration.global());

    String filename = "/test";

    writeIncreasingBytesToFile(fs, "/test", workerMemoryBytes,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.ASYNC_THROUGH)
            .setBlockSizeBytes(4)
            .build());

    CommonUtils.waitFor(String.format("%s to be persisted", filename) , () -> {
      try {
        return fs.getStatus(new AlluxioURI(filename)).isPersisted();
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000)
        .setInterval(Constants.SECOND_MS));
  }

  protected void writeIncreasingBytesToFile(FileSystem fs, String filePath, int fileLen,
      CreateFilePOptions op)
      throws Exception {
    try (FileOutStream os = fs.createFile(new AlluxioURI(filePath), op)) {
      for (int k = 0; k < fileLen; k++) {
        os.write((byte) k);
      }
    } catch (Exception e) {
      System.out.println("oh no");
    }
  }
}
