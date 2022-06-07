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

package alluxio.worker.block;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import alluxio.ConfigurationRule;
import alluxio.StorageTierAssoc;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.retry.RetryPolicy;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("unchecked")
public class BlockMasterSyncTest {

  @Rule
  public final ConfigurationRule mConfigurationRule = new ConfigurationRule(
      new ImmutableMap.Builder<PropertyKey, Object>()
        .put(PropertyKey.TEST_MODE, true)
        .build(),
      Configuration.modifiableGlobal()
  );

  private final BlockStoreMeta mTestBlockStoreMeta = new TestBlockMeta();
  private final BlockHeartbeatReport mTestReport =
      new BlockHeartbeatReport(ImmutableMap.of(), ImmutableList.of(), ImmutableMap.of());
  // mocked dependencies of BlockMasterSync
  private BlockWorker mBlockWorker;
  private AtomicReference<Long> mBlockWorkerId;
  private WorkerNetAddress mWorkerNetAddress;
  private BlockMasterClientPool mBlockMasterClientPool;
  private BlockMasterClient mClient;

  // closer to manage BlockMasterSync instances
  Closer mCloser = Closer.create();

  @Before
  public void before() throws Exception {
    mBlockWorker = mock(BlockWorker.class);
    when(mBlockWorker.getStoreMeta()).thenReturn(mTestBlockStoreMeta);
    when(mBlockWorker.getStoreMetaFull()).thenReturn(mTestBlockStoreMeta);
    when(mBlockWorker.getReport()).thenReturn(mTestReport);

    mBlockWorkerId = new AtomicReference<>(1L);
    mWorkerNetAddress = new WorkerNetAddress();
    // set up mock client, specific behaviors are left for test functions
    // to define
    mClient = mock(BlockMasterClient.class);
    // set up mock client pool to return our mock Client
    mBlockMasterClientPool = mock(BlockMasterClientPool.class);
    when(mBlockMasterClientPool.acquire()).thenReturn(mClient);
  }

  @Test
  public void testFailToAcquireLease() throws Exception {
    // our client will throw error when acquiring lease,
    // simulating failure
    doThrow(new FailedToAcquireRegisterLeaseException("Failed Acquiring Lease"))
        .when(mClient)
        .acquireRegisterLeaseWithBackoff(
            any(long.class),
            any(int.class),
            any(RetryPolicy.class)
        );

    // set configuration to require lease
    Configuration.set(PropertyKey.WORKER_REGISTER_LEASE_ENABLED, true);
    Configuration.set(PropertyKey.WORKER_REGISTER_LEASE_RETRY_MAX_DURATION, "500ms");
    Configuration.set(PropertyKey.WORKER_REGISTER_LEASE_RETRY_SLEEP_MIN, "100ms");
    Configuration.set(PropertyKey.WORKER_REGISTER_LEASE_RETRY_SLEEP_MAX, "100ms");

    Throwable t = null;
    try {
      BlockMasterSync sync =
          mCloser.register(new BlockMasterSync(
              mBlockWorker, mBlockWorkerId, mWorkerNetAddress, mBlockMasterClientPool));
      fail(String.format("%s should error because of failure to acquire lease", sync));
    } catch (Throwable e) {
      t = e;
    }

    assertTrue(t.getMessage().toLowerCase().contains("register lease timeout exceeded"));
  }

  @Test
  public void testFailToRegister() throws Exception {
    String testMessage = "Testing failure to register";
    // make client throw an error when registering
    doThrow(new IOException(testMessage))
        .when(mClient)
        .registerWithStream(
            any(long.class),
            any(List.class),
            any(Map.class),
            any(Map.class),
            any(Map.class),
            any(Map.class),
            any(List.class)
        );

    Configuration.set(PropertyKey.WORKER_REGISTER_LEASE_ENABLED, false);
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);

    IOException t = null;
    try {
      BlockMasterSync sync =
          new BlockMasterSync(
              mBlockWorker, mBlockWorkerId, mWorkerNetAddress, mBlockMasterClientPool);
      fail(String.format("%s should error because of failure to register", sync));
    } catch (IOException e) {
      t = e;
    }

    assertTrue(t.getMessage().contains(testMessage));
  }

  @Test
  public void testHeartbeatTimeout() throws Exception {
    String testMessage = "Testing heartbeat failure";
    // simulate heartbeat failure
    doThrow(new IOException(testMessage))
        .when(mClient)
        .heartbeat(
            any(long.class),
            any(Map.class),
            any(Map.class),
            any(List.class),
            any(Map.class),
            any(Map.class),
            any(List.class)
        );

    Configuration.set(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS, 100);

    BlockMasterSync sync =
        mCloser.register(new BlockMasterSync(
            mBlockWorker, mBlockWorkerId, mWorkerNetAddress, mBlockMasterClientPool));

    // wait pass heartbeat interval so that next heartbeat failure would result
    // in timeout
    Thread.sleep(200);

    RuntimeException t = assertThrows(RuntimeException.class, sync::heartbeat);
    assertTrue(t.getMessage().contains("heartbeat timeout exceeded"));
  }

  @Test
  public void testMasterFreeCommand() throws Exception {
    // simulate master returning a FREE command
    List<Long> toFreeBlocks = ImmutableList.of(1L, 2L, 3L, 4L);
    Command freeCmd = Command
        .newBuilder()
        .setCommandType(CommandType.Free)
        .addAllData(toFreeBlocks)
        .build();

    doReturn(freeCmd)
        .when(mClient)
        .heartbeat(
            any(long.class),
            any(Map.class),
            any(Map.class),
            any(List.class),
            any(Map.class),
            any(Map.class),
            any(List.class)
        );

    BlockMasterSync sync =
        mCloser.register(new BlockMasterSync(
            mBlockWorker, mBlockWorkerId, mWorkerNetAddress, mBlockMasterClientPool));

    // in this heartbeat sync will receive a FREE command
    sync.heartbeat();

    // wait for some time to let the async block remover finish its work
    Thread.sleep(200);

    for (Long blockId: toFreeBlocks) {
      verify(mBlockWorker).removeBlock(any(long.class), eq(blockId));
    }
  }

  @Test
  public void testRegisterCommand() throws Exception {
    // simulate a re-registration command from master
    doReturn(mBlockWorkerId.get())
        .when(mClient)
        .getId(any(WorkerNetAddress.class));
    doReturn(Command.newBuilder().setCommandType(CommandType.Register).build())
        .when(mClient)
        .heartbeat(
            any(long.class),
            any(Map.class),
            any(Map.class),
            any(List.class),
            any(Map.class),
            any(Map.class),
            any(List.class)
        );

    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, false);
    Configuration.set(PropertyKey.WORKER_REGISTER_LEASE_ENABLED, false);

    BlockMasterSync sync =
        mCloser.register(new BlockMasterSync(
            mBlockWorker, mBlockWorkerId, mWorkerNetAddress, mBlockMasterClientPool));

    // in this heartbeat sync will receive a REGISTER command
    sync.heartbeat();

    // verify that sync made the registration with the correct parameters
    // by calling the register method on client for 2 times,
    // one on creation and another time responding to master's REGISTER command
    verify(mClient, times(2)).register(
        eq(mBlockWorkerId.get()),
        any(List.class),
        any(Map.class),
        any(Map.class),
        any(Map.class),
        any(Map.class),
        any(List.class)
    );
  }

  @After
  public void after() throws Exception {
    mCloser.close();
    Configuration.reloadProperties();
  }

  // Dumb implementation of BlockStoreMeta that returns
  // fabricated metadata
  private static class TestBlockMeta implements BlockStoreMeta {

    @Override
    public Map<String, List<Long>> getBlockList() {
      return ImmutableMap.of("MEM", ImmutableList.of(1L, 2L, 3L));
    }

    @Override
    public Map<BlockStoreLocation, List<Long>> getBlockListByStorageLocation() {
      return ImmutableMap.of(
          new BlockStoreLocation("MEM", 0, "MEM"),
          ImmutableList.of(1L, 2L),
          new BlockStoreLocation("MEM", 1, "MEM"),
          ImmutableList.of(3L)
      );
    }

    @Override
    public long getCapacityBytes() {
      return 1024 * 1024 * 1024L;
    }

    @Override
    public Map<String, Long> getCapacityBytesOnTiers() {
      return ImmutableMap.of("MEM", 1024 * 1024 * 1024L);
    }

    @Override
    public Map<Pair<String, String>, Long> getCapacityBytesOnDirs() {
      return ImmutableMap.of(
          new Pair<>("MEM", "/test/dir0"), 512 * 1024 * 1024L,
          new Pair<>("MEM", "/test/dir1"), 512 * 1024 * 1024L
      );
    }

    @Override
    public Map<String, List<String>> getDirectoryPathsOnTiers() {
      return ImmutableMap.of(
          "MEM",
          ImmutableList.of("/test/dir0", "/test/dir1")
      );
    }

    @Override
    public Map<String, List<String>> getLostStorage() {
      return ImmutableMap.of();
    }

    @Override
    public int getNumberOfBlocks() {
      return 3;
    }

    @Override
    public long getUsedBytes() {
      return 512 * 1024 * 1024L;
    }

    @Override
    public Map<String, Long> getUsedBytesOnTiers() {
      return ImmutableMap.of(
          "MEM", 512 * 1024 * 1024L
      );
    }

    @Override
    public Map<Pair<String, String>, Long> getUsedBytesOnDirs() {
      return ImmutableMap.of(
          new Pair<>("MEM", "/test/dir0"), 256 * 1024 * 1024L,
          new Pair<>("MEM", "/test/dir1"), 256 * 1024 * 1024L
          );
    }

    @Override
    public StorageTierAssoc getStorageTierAssoc() {
      return new StorageTierAssoc() {
        @Override
        public String getAlias(int ordinal) {
          return "MEM";
        }

        @Override
        public int getOrdinal(String alias) {
          return 0;
        }

        @Override
        public int size() {
          return 1;
        }

        @Override
        public List<String> getOrderedStorageAliases() {
          return ImmutableList.of("MEM");
        }

        @Override
        public List<Pair<BlockStoreLocation, BlockStoreLocation>> intersectionList() {
          return ImmutableList.of();
        }
      };
    }
  }
}
