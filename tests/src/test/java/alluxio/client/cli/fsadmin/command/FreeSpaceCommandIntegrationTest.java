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

package alluxio.client.cli.fsadmin.command;

import alluxio.AlluxioURI;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.util.SleepUtils;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Test;

public class FreeSpaceCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void workerNotRunning() throws Exception {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();

    mLocalAlluxioCluster.stopWorkers();
    int ret = mFsAdminShell.run(
        "freeSpace", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertNotEquals(0, ret);
  }

  @Test
  public void freeSpaceWhenWorkerIsEmpty() {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    int ret = mFsAdminShell.run(
        "freeSpace", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertEquals(0, ret);
  }

  @Test
  public void freeSpaceWhenWorkerHaveEnoughSpace() throws Exception {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    String fileName = "/testFile";
    FileSystem client = mLocalAlluxioCluster.getClient();
    FileSystemTestUtils.createByteFile(client, fileName, WritePType.CACHE_THROUGH, 10);
    int ret = mFsAdminShell.run(
        "freeSpace", "--percent", "1", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertEquals(0, ret);
    // wait for block heartbeat
    SleepUtils.sleepMs(
        Configuration.global().getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    Assert.assertEquals(100, client.getStatus(new AlluxioURI(fileName)).getInAlluxioPercentage());
  }

  @Test
  public void freeSpaceWhenWorkerHaveNoEnoughSpace() throws Exception {
    WorkerNetAddress workerAddress = mLocalAlluxioCluster.getWorkerAddress();
    String fileName = "/testFile";
    FileSystem client = mLocalAlluxioCluster.getClient();
    FileSystemTestUtils.createByteFile(client, fileName, WritePType.CACHE_THROUGH, 10);
    int ret = mFsAdminShell.run(
        "freeSpace", workerAddress.getHost() + ":" + workerAddress.getRpcPort());
    Assert.assertEquals(0, ret);
    // wait for block heartbeat
    SleepUtils.sleepMs(
        Configuration.global().getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    Assert.assertEquals(0, client.getStatus(new AlluxioURI(fileName)).getInAlluxioPercentage());
  }
}
