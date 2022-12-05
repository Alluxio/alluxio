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

import static org.junit.Assert.assertEquals;

import alluxio.clock.ManualClock;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Executors;

public class BlockMasterDeleteLostWorkerIntegrationTest {
  private BlockMaster mBlockMaster;
  private ManualClock mClock;
  private MasterRegistry mRegistry;

  private static final int MASTER_WORKER_DELETE_TIMEOUT_MS = 10;
  private static final int MASTER_WORKER_TIMEOUT_MS = 10;
  static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress()
          .setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82);

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.MASTER_LOST_WORKER_DELETION_TIMEOUT_MS,
        MASTER_WORKER_DELETE_TIMEOUT_MS);
    Configuration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, MASTER_WORKER_TIMEOUT_MS);

    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    MetricsMaster metricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, metricsMaster);
    mClock = new ManualClock();

    mBlockMaster = new DefaultBlockMaster(metricsMaster, masterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(
            Executors.newFixedThreadPool(10)));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void lostWorkerDeletedAfterTimeout() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(workerId, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        RegisterWorkerPOptions.getDefaultInstance());
    DefaultBlockMaster.LostWorkerDetectionHeartbeatExecutor lostWorkerDetector =
          ((DefaultBlockMaster) mBlockMaster).new LostWorkerDetectionHeartbeatExecutor();
    // Verify the worker
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkerCount());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);

    // The worker will not be deleted, if the lost time is less than MASTER_WORKER_TIMEOUT_MS
    long newTimeMs = worker.getLastUpdatedTimeMs() + MASTER_WORKER_TIMEOUT_MS + 1;
    mClock.setTimeMs(newTimeMs);
    lostWorkerDetector.heartbeat();
    assertEquals(0, mBlockMaster.getWorkerCount());
    assertEquals(1, mBlockMaster.getLostWorkerCount());

    // The worker will be deleted, if the lost time is greater than MASTER_WORKER_TIMEOUT_MS
    newTimeMs = newTimeMs + MASTER_WORKER_DELETE_TIMEOUT_MS + 1;
    mClock.setTimeMs(newTimeMs);
    lostWorkerDetector.heartbeat();
    assertEquals(0, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkerCount());
  }
}
