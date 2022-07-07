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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.clock.ManualClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.grpc.BlockHeartbeatPRequest;
import alluxio.grpc.BlockHeartbeatPResponse;
import alluxio.grpc.BlockIdList;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.util.SleepUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.RegisterLease;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BlockMasterWorkerServiceHandlerTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);

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
    initServiceHandler(true);
  }

  public void initServiceHandler(boolean leaseEnabled) throws Exception {
    if (leaseEnabled) {
      Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, true);
      Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_TTL, "3s");
      // Tests on the JVM check logic will be done separately
      Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_RESPECT_JVM_SPACE, false);
      Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_COUNT, 1);
    } else {
      Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false);
    }

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
  public void registerWithNoLeaseIsRejected() {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

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

    RegisterWorkerPRequest request = RegisterWorkerPRequest.newBuilder()
        .setWorkerId(workerId)
        .addStorageTiers("MEM")
        .putTotalBytesOnTiers("MEM", 1000L).putUsedBytesOnTiers("MEM", 0L)
        .setOptions(RegisterWorkerPOptions.getDefaultInstance())
        .addCurrentBlocks(listEntry1)
        .build();

    Queue<Throwable> errors = new ConcurrentLinkedDeque<>();
    StreamObserver<RegisterWorkerPResponse> noopResponseObserver =
        new StreamObserver<RegisterWorkerPResponse>() {
          @Override
          public void onNext(RegisterWorkerPResponse response) {}

          @Override
          public void onError(Throwable t) {
            errors.offer(t);
          }

          @Override
          public void onCompleted() {}
        };

    // The responseObserver should see an error
    mHandler.registerWorker(request, noopResponseObserver);
    assertEquals(1, errors.size());
    Throwable t = errors.poll();
    Assert.assertThat(t.getMessage(),
        containsString("does not have a lease or the lease has expired."));
  }

  @Test
  public void registerWorkerFailsOnDuplicateBlockLocation() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

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

    // Prepare a lease
    GetRegisterLeasePRequest leaseRequest = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(workerId)
        .setBlockCount(blockIdList1.getBlockIdCount() + blockIdList2.getBlockIdCount())
        .build();
    Optional<RegisterLease> lease = mBlockMaster.tryAcquireRegisterLease(leaseRequest);
    assertTrue(lease.isPresent());

    // The request is not deduplicated
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

    mBlockMaster.releaseRegisterLease(workerId);
  }

  @Test
  public void registerLeaseExpired() {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

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

    // Prepare a lease
    GetRegisterLeasePRequest leaseRequest = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(workerId)
        .setBlockCount(blockIdList1.getBlockIdCount())
        .build();
    Optional<RegisterLease> lease = mBlockMaster.tryAcquireRegisterLease(leaseRequest);
    assertTrue(lease.isPresent());

    // Sleep for a while so that the lease expires
    SleepUtils.sleepMs(5000);

    // The lease is recycled and taken away
    GetRegisterLeasePRequest newLeaseRequest = GetRegisterLeasePRequest.newBuilder()
        .setWorkerId(workerId + 1)
        .setBlockCount(blockIdList1.getBlockIdCount())
        .build();
    Optional<RegisterLease> newLease = mBlockMaster.tryAcquireRegisterLease(newLeaseRequest);
    assertTrue(newLease.isPresent());

    RegisterWorkerPRequest request = RegisterWorkerPRequest.newBuilder()
        .setWorkerId(workerId)
        .addStorageTiers("MEM")
        .putTotalBytesOnTiers("MEM", 1000L).putUsedBytesOnTiers("MEM", 0L)
        .setOptions(RegisterWorkerPOptions.getDefaultInstance())
        .addCurrentBlocks(listEntry1)
        .build();

    // Noop response observer
    Queue<Throwable> errors = new ConcurrentLinkedDeque<>();
    StreamObserver<RegisterWorkerPResponse> noopResponseObserver =
        new StreamObserver<RegisterWorkerPResponse>() {
          @Override
          public void onNext(RegisterWorkerPResponse response) {}

          @Override
          public void onError(Throwable t) {
            errors.offer(t);
          }

          @Override
          public void onCompleted() {}
        };

    mHandler.registerWorker(request, noopResponseObserver);
    assertEquals(1, errors.size());
    Throwable t = errors.poll();
    Assert.assertThat(t.getMessage(),
        containsString("does not have a lease or the lease has expired."));

    mBlockMaster.releaseRegisterLease(workerId + 1);
  }

  @Test
  public void registerLeaseTurnedOff() throws Exception {
    initServiceHandler(false);

    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

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

    // No lease is acquired
    RegisterWorkerPRequest request = RegisterWorkerPRequest.newBuilder()
        .setWorkerId(workerId)
        .addStorageTiers("MEM")
        .putTotalBytesOnTiers("MEM", 1000L).putUsedBytesOnTiers("MEM", 0L)
        .setOptions(RegisterWorkerPOptions.getDefaultInstance())
        .addCurrentBlocks(listEntry1)
        .build();

    Queue<Throwable> errors = new ConcurrentLinkedDeque<>();
    StreamObserver<RegisterWorkerPResponse> noopResponseObserver =
        new StreamObserver<RegisterWorkerPResponse>() {
          @Override
          public void onNext(RegisterWorkerPResponse response) {}

          @Override
          public void onError(Throwable t) {
            errors.offer(t);
          }

          @Override
          public void onCompleted() {}
        };

    mHandler.registerWorker(request, noopResponseObserver);
    assertEquals(0, errors.size());
  }

  @Test
  public void workerHeartbeatFailsOnDuplicateBlockLocation() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

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
