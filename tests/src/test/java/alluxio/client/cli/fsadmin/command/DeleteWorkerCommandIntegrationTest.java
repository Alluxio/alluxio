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

import alluxio.Constants;
import alluxio.cli.fsadmin.command.DeleteWorkerCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class DeleteWorkerCommandIntegrationTest  extends AbstractFsAdminShellTest {
  static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress()
      .setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82);

  @Test
  public void noArg() {
    int ret = mFsAdminShell.run("deleteWorker");
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();
    Assert.assertTrue(output.contains(DeleteWorkerCommand.usage()));
  }

  @Test
  public void notExistWorker() {
    String notExistWorkerName = "notExistWorkerId";
    int ret = mFsAdminShell.run("deleteWorker", notExistWorkerName);
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertTrue(output.contains("failed"));
  }

  @Test
  public void deleteLostWorker() throws Exception {
    DefaultBlockMaster blockMaster =
        (DefaultBlockMaster) mLocalAlluxioCluster.getLocalAlluxioMaster()
        .getMasterProcess().getMaster(BlockMaster.class);
    String notDeletedWorker = mLocalAlluxioCluster.getWorkerAddress().getHost();
    // Register a fake worker, this worker will not have heartbeat,
    // will not re-register, easy to test
    long workerId = blockMaster.getWorkerId(NET_ADDRESS_1);
    Assert.assertEquals(1, blockMaster.getWorkerCount());
    blockMaster.workerRegister(workerId, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        RegisterWorkerPOptions.getDefaultInstance());
    Assert.assertEquals(2, blockMaster.getWorkerCount());
    blockMaster.forgetWorker(workerId);
    Assert.assertEquals(1, blockMaster.getLostWorkerCount());

    int ret = mFsAdminShell.run("deleteWorker", NET_ADDRESS_1.getHost());

    Assert.assertEquals(0, ret);
    Assert.assertEquals(1, blockMaster.getWorkerCount());
    Assert.assertEquals(0, blockMaster.getLostWorkerCount());
    Assert.assertEquals(notDeletedWorker, mLocalAlluxioCluster.getWorkerAddress().getHost());
    String output = mOutput.toString();
    Assert.assertTrue(output.contains("has been deleted"));
  }

  @Test
  public void deleteNotLostWorker() {
    // Without the --force flag with command, a non-lost worker will not be deleted
    BlockMaster blockMaster = mLocalAlluxioCluster.getLocalAlluxioMaster()
        .getMasterProcess().getMaster(BlockMaster.class);

    int ret = mFsAdminShell.run("deleteWorker", mLocalAlluxioCluster.getWorkerAddress().getHost());

    Assert.assertEquals(0, ret);
    Assert.assertEquals(1, blockMaster.getWorkerCount());
    String output = mOutput.toString();
    Assert.assertTrue(output.contains("is not a LOST Worker"));
  }

  @Test
  public void forceDeleteNotLostWorker() throws Exception {
    // Prevent worker timeouts
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, Constants.HOUR_MS);
    // If there is a force flag, a non-lost worker will be deleted
    BlockMaster blockMaster = mLocalAlluxioCluster.getLocalAlluxioMaster()
        .getMasterProcess().getMaster(BlockMaster.class);
    String notDeletedWorker = mLocalAlluxioCluster.getWorkerAddress().getHost();
    long workerId = blockMaster.getWorkerId(NET_ADDRESS_1);
    Assert.assertEquals(1, blockMaster.getWorkerCount());

    // Forcibly delete an active state worker, the worker will re-register to the Master
    // So, register a fake worker, this worker has no real process,
    // which can prevent the worker re-register to the master, easy for testing
    blockMaster.workerRegister(workerId, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        RegisterWorkerPOptions.getDefaultInstance());
    Assert.assertEquals(2, blockMaster.getWorkerCount());
    // Make sure the worker is on alive
    blockMaster.workerHeartbeat(workerId, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(1L),
        ImmutableMap.of(), ImmutableMap.of(), Lists.newArrayList());
    Assert.assertEquals(2, blockMaster.getWorkerCount());
    Assert.assertEquals(0, blockMaster.getLostWorkerCount());

    int ret = mFsAdminShell.run("deleteWorker", NET_ADDRESS_1.getHost(), "--force");

    Assert.assertEquals(0, ret);
    Assert.assertEquals(1, blockMaster.getWorkerCount());
    Assert.assertEquals(0, blockMaster.getLostWorkerCount());
    Assert.assertEquals(notDeletedWorker, mLocalAlluxioCluster.getWorkerAddress().getHost());
    String output = mOutput.toString();
    Assert.assertTrue(output.contains("has been deleted"));
  }
}
