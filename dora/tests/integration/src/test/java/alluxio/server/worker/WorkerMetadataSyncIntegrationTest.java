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

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.WaitForOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

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
          new ImmutableMap.Builder<PropertyKey, Object>()
                  // make heartbeat interval shorter for fast testing
                  .put(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, 100)
                  .build(),
          Configuration.modifiableGlobal());

  /* Not applied as registration is not going thru master any more */
  @Test
  @Ignore
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

  /* Not applied as registration is not going thru master any more */
  @Test
  @Ignore
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
}
