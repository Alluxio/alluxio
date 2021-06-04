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

package alluxio.master.block;

import static org.junit.Assert.assertThrows;

import alluxio.clock.ManualClock;
import alluxio.grpc.BlockHeartbeatPRequest;
import alluxio.grpc.BlockHeartbeatPResponse;
import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BlockMasterWorkerServiceHandlerTest {
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private MetricsMaster mMetricsMaster;
  private BlockMasterWorkerServiceHandler mHandler;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mClock = new ManualClock();
    mExecutorService =
            Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
    mHandler = new BlockMasterWorkerServiceHandler(mBlockMaster);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void registerWorkerFailsOnDuplicateBlockLocation() throws Exception {
    long workerId = 1L;

    // Prepare LocationBlockIdListEntry objects
    BlockStoreLocation loc = new BlockStoreLocation("MEM", 0);
    BlockStoreLocationProto locationProto = BlockStoreLocationProto.newBuilder()
            .setTierAlias(loc.tierAlias())
            .setMediumType(loc.mediumType())
            .build();

    BlockIdList blockIdList1 = BlockIdList.newBuilder()
            .addAllBlockId(ImmutableList.of(1L, 2L)).build();
    LocationBlockIdListEntry listEntry1 = LocationBlockIdListEntry.newBuilder()
            .setKey(locationProto).setValue(blockIdList1).build();

    BlockIdList blockIdList2 = BlockIdList.newBuilder()
            .addAllBlockId(ImmutableList.of(3L, 4L)).build();
    LocationBlockIdListEntry listEntry2 = LocationBlockIdListEntry.newBuilder()
            .setKey(locationProto).setValue(blockIdList2).build();

    // The request is not deduplicated, the
    RegisterWorkerPRequest request = RegisterWorkerPRequest.newBuilder()
            .setWorkerId(workerId)
            .addStorageTiers("MEM")
            .putTotalBytesOnTiers("MEM", 1000L).putUsedBytesOnTiers("MEM", 0L)
            .setOptions(RegisterWorkerPOptions.getDefaultInstance())
            .addCurrentBlocks(listEntry1)
            .addCurrentBlocks(listEntry2)
            .build();

    // Noop response observer
    StreamObserver<RegisterWorkerPResponse> noopResponseObserver =
        new StreamObserver<RegisterWorkerPResponse>() {
          @Override
          public void onNext(RegisterWorkerPResponse response) {}

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };

    assertThrows(AssertionError.class, () -> {
      mHandler.registerWorker(request, noopResponseObserver);
    });
  }

  @Test
  public void workerHeartbeatFailsOnDuplicateBlockLocation() throws Exception {
    long workerId = 1L;

    // Prepare LocationBlockIdListEntry objects
    BlockStoreLocation loc = new BlockStoreLocation("MEM", 0);
    BlockStoreLocationProto locationProto = BlockStoreLocationProto.newBuilder()
            .setTierAlias(loc.tierAlias())
            .setMediumType(loc.mediumType())
            .build();

    BlockIdList blockIdList1 = BlockIdList.newBuilder()
            .addAllBlockId(ImmutableList.of(1L, 2L)).build();
    LocationBlockIdListEntry listEntry1 = LocationBlockIdListEntry.newBuilder()
            .setKey(locationProto).setValue(blockIdList1).build();

    BlockIdList blockIdList2 = BlockIdList.newBuilder()
            .addAllBlockId(ImmutableList.of(3L, 4L)).build();
    LocationBlockIdListEntry listEntry2 = LocationBlockIdListEntry.newBuilder()
            .setKey(locationProto).setValue(blockIdList2).build();

    BlockHeartbeatPRequest request = BlockHeartbeatPRequest.newBuilder()
            .setWorkerId(workerId)
            .addAddedBlocks(listEntry1)
            .addAddedBlocks(listEntry2)
            .build();

    // Noop response observer
    StreamObserver<BlockHeartbeatPResponse> noopResponseObserver =
        new StreamObserver<BlockHeartbeatPResponse>() {
          @Override
          public void onNext(BlockHeartbeatPResponse response) {}

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };

    assertThrows(AssertionError.class, () -> {
      mHandler.blockHeartbeat(request, noopResponseObserver);
    });
  }
}
