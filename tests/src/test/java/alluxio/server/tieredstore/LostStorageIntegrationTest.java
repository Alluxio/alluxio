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

package alluxio.server.tieredstore;

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests for getting worker lost storage information.
 */
public class LostStorageIntegrationTest extends BaseIntegrationTest {
  private static final int CAPACITY_BYTES = Constants.KB;
  private static final String SSD_TIER = "SSD";
  private static final String HDD_TIER = "HDD";
  private static final String WORKER_STORAGE_SUFFIX = "/alluxioworker";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setStartCluster(false).build();

  private LocalAlluxioCluster mLocalAlluxioCluster = null;

  private BlockMasterClient mBlockMasterClient = null;

  @Test
  public void reportLostStorageInWorkerRegister() throws Exception {
    File ssdDir = Files.createTempDir();
    String ssdPath = ssdDir.getAbsolutePath();
    File hddDir = Files.createTempDir();
    String hddPath = hddDir.getAbsolutePath();

    // Set to read only so worker storage paths cannot be initialize
    ssdDir.setReadOnly();
    hddDir.setReadOnly();

    startClusterWithWorkerStorage(ssdPath, hddPath);
    checkLostStorageResults(ssdPath, hddPath);
  }

  @Test
  public void reportLostStorageInHeartbeat() throws Exception {
    File ssdDir = Files.createTempDir();
    String ssdPath = ssdDir.getAbsolutePath();
    File hddDir = Files.createTempDir();
    String hddPath = hddDir.getAbsolutePath();

    startClusterWithWorkerStorage(ssdPath, hddPath);

    FileUtils.deleteDirectory(ssdDir);
    FileUtils.deleteDirectory(hddDir);

    // Make sure worker lost storage is detected and heartbeat with the master
    Thread.sleep(10 * ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    checkLostStorageResults(ssdPath, hddPath);
  }

  @Test
  public void lostStorageWhenRestart() throws Exception {
    File ssdDir = Files.createTempDir();
    String ssdPath = ssdDir.getAbsolutePath();
    File hddDir = Files.createTempDir();
    String hddPath = hddDir.getAbsolutePath();

    // Set to read only so worker storage paths cannot be initialize
    ssdDir.setReadOnly();
    startClusterWithWorkerStorage(ssdPath, hddPath);

    FileUtils.deleteDirectory(hddDir);
    // Make sure lost storage is detected and reported to master
    Thread.sleep(10 * ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    checkLostStorageResults(ssdPath, hddPath);

    mLocalAlluxioCluster.restartMasters();
    mLocalAlluxioCluster.waitForWorkersRegistered(6 * Constants.SECOND_MS);
    checkLostStorageResults(ssdPath, hddPath);
  }

  /**
   * Starts the {@link LocalAlluxioCluster}.
   *
   * @param ssdPath the local path representing a worker storage in ssd tier
   * @param hddPath the local path representing a worker storage in hdd tier
   */
  private void startClusterWithWorkerStorage(String ssdPath, String hddPath) throws Exception {
    mLocalAlluxioClusterResource
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1KB")
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "3")
        .setProperty(PropertyKey.WORKER_MEMORY_SIZE, CAPACITY_BYTES)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1), SSD_TIER)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(2), HDD_TIER)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
            Files.createTempDir().getAbsolutePath())
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1),
            ssdPath)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
            String.valueOf(CAPACITY_BYTES))
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(2),
            hddPath)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(2),
            String.valueOf(CAPACITY_BYTES));
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mBlockMasterClient =
        BlockMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    mBlockMasterClient.connect();
  }

  /**
   * Checks if the worker lost storage results are as expected.
   *
   * @param ssdPath the local path representing a lost worker storage in ssd tier
   * @param hddPath the local path representing a lost worker storage in hdd tier
   */
  private void checkLostStorageResults(String ssdPath, String hddPath) throws IOException {
    List<WorkerLostStorageInfo>  infoList = mBlockMasterClient.getWorkerLostStorage();
    Assert.assertEquals(1, infoList.size());
    Map<String, StorageList> lostStorageMapTwo = infoList.get(0).getLostStorageMap();
    Assert.assertEquals(ssdPath + WORKER_STORAGE_SUFFIX,
        lostStorageMapTwo.get(SSD_TIER).getStorage(0));
    Assert.assertEquals(hddPath + WORKER_STORAGE_SUFFIX,
        lostStorageMapTwo.get(HDD_TIER).getStorage(0));
  }
}
