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

package alluxio.server.worker;

import static alluxio.util.CommonUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.WaitForOptions;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.DefaultBlockWorker;

import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class WorkerMetadataSyncIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
          new LocalAlluxioClusterResource.Builder()
                  // don't start cluster so that test methods can set different configurations
                  // before starting
                  .setStartCluster(false)
                  .build();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(
          ImmutableMap.of(), Configuration.modifiableGlobal());

  @Test
  public void reRegisterWorker() throws Exception {
    mLocalAlluxioClusterResource.start();

    DefaultBlockMaster master = (DefaultBlockMaster) mLocalAlluxioClusterResource.get()
            .getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
    // forget all workers and let the worker re-register through
    // syncing heartbeat
    master.forgetAllWorkers();
    // master should now recognize no workers
    assertEquals(0, master.getWorkerCount());

    // wait for re-registration to complete
    waitFor("Wait for Worker Registration",
            () -> master.getWorkerCount() == 1, WaitForOptions.defaults().setTimeoutMs(2000));
  }

  @Test
  public void acquireLeaseNoStreaming() throws Exception {
    // test that registration works when lease is enabled and streaming is disabled
    mConfigurationRule.set(PropertyKey.WORKER_REGISTER_LEASE_ENABLED, true);
    mConfigurationRule.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, false);
    mLocalAlluxioClusterResource.start();

    DefaultBlockMaster master = (DefaultBlockMaster) mLocalAlluxioClusterResource.get()
            .getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
    // check registration success
    assertEquals(1, master.getWorkerCount());
  }

  @Test
  public void freeBlocks() throws Exception {
    mLocalAlluxioClusterResource.start();

    DefaultBlockMaster master = (DefaultBlockMaster) mLocalAlluxioClusterResource.get()
            .getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
    DefaultBlockWorker worker = (DefaultBlockWorker) mLocalAlluxioClusterResource.get()
            .getWorkerProcess().getWorker(BlockWorker.class);

    // create & commit a block
    long session = 1L;
    long block = 1000L;
    worker.createBlock(session, block, 0, new CreateBlockOptions(null, null, 1));
    worker.commitBlock(session, block, false);
    assertTrue(worker.getBlockStore().hasBlockMeta(block));

    // remove the block on master, and worker should remove this block through
    // the syncing mechanism
    master.removeBlocks(ImmutableList.of(block), false);
    waitFor("Wait for blocks to be freed at worker",
            () -> !worker.getBlockStore().hasBlockMeta(block),
            WaitForOptions.defaults().setTimeoutMs(2000));
  }
}
