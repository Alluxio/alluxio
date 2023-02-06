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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.ClientContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.Metric;
import alluxio.master.MasterClientContext;
import alluxio.master.SingleMasterInquireClient;
import alluxio.retry.RetryPolicy;
import alluxio.wire.WorkerNetAddress;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class SpecificMasterBlockSyncTest {
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void heartbeatThread() throws Exception {
    // Flaky registration succeeds every other time.
    TestBlockMasterClient.INSTANCE.setFlakyRegistration(true);
    TestBlockMasterClient.INSTANCE.setReturnRegisterCommand(false);

    SpecificMasterBlockSync sync = new SpecificMasterBlockSync(
        getMockedBlockWorker(), new AtomicReference<>(0L), WorkerNetAddress.DUMMY, mClientPool,
        new BlockHeartbeatReporter()
    );
    assertFalse(sync.isRegistered());

    // heartbeat registers the worker if it has not been registered.
    sync.heartbeat();
    assertTrue(sync.isRegistered());

    // heartbeat returning register command resets the worker state.
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);
    TestBlockMasterClient.INSTANCE.setReturnRegisterCommand(true);
    sync.heartbeat();
    TestBlockMasterClient.INSTANCE.setReturnRegisterCommand(false);
    assertFalse(sync.isRegistered());

    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, false);
    TestBlockMasterClient.INSTANCE.setReturnRegisterCommand(true);
    sync.heartbeat();
    TestBlockMasterClient.INSTANCE.setReturnRegisterCommand(false);
    assertFalse(sync.isRegistered());

    // heartbeat registers the worker if it has not been registered.
    sync.heartbeat();
    assertTrue(sync.isRegistered());

    assertTrue(TestBlockMasterClient.INSTANCE.mRegisterCalled);
    assertTrue(TestBlockMasterClient.INSTANCE.mRegisterWithStreamCalled);
  }

  private static class TestBlockMasterClient extends BlockMasterClient {
    public static final TestBlockMasterClient INSTANCE = new TestBlockMasterClient();

    private boolean mLastRegisterSuccess = true;
    private boolean mFlakyRegistration = false;
    private boolean mReturnRegisterCommand = false;

    private boolean mRegisterCalled = false;

    private boolean mRegisterWithStreamCalled = false;

    public void setFlakyRegistration(boolean value) {
      mFlakyRegistration = value;
    }

    public void setReturnRegisterCommand(boolean value) {
      mReturnRegisterCommand = value;
    }

    public TestBlockMasterClient() {
      super(MasterClientContext
          .newBuilder(ClientContext.create(Configuration.global()))
          .setMasterInquireClient(new SingleMasterInquireClient(
              InetSocketAddress.createUnresolved("localhost", 0))).build());
    }

    @Override
    public void register(
        long workerId, List<String> storageTierAliases,
        Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
        Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
        Map<String, List<String>> lostStorage, List<ConfigProperty> configList)
        throws IOException {
      if (!mFlakyRegistration) {
        return;
      }
      if (mLastRegisterSuccess) {
        mLastRegisterSuccess = false;
        throw new IOException("Registration failed");
      } else {
        mLastRegisterSuccess = true;
        mRegisterCalled = true;
      }
    }

    @Override
    public void registerWithStream(
        long workerId, List<String> storageTierAliases,
        Map<String, Long> totalBytesOnTiers,
        Map<String, Long> usedBytesOnTiers,
        Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
        Map<String, List<String>> lostStorage,
        List<ConfigProperty> configList) throws IOException {
      if (!mFlakyRegistration) {
        return;
      }
      if (mLastRegisterSuccess) {
        mLastRegisterSuccess = false;
        throw new IOException("Registration failed");
      } else {
        mLastRegisterSuccess = true;
        mRegisterWithStreamCalled = true;
      }
    }

    @Override
    public void addWorkerId(long workerId, WorkerNetAddress address) throws IOException {
    }

    @Override
    public synchronized Command heartbeat(
        long workerId, Map<String, Long> capacityBytesOnTiers,
        Map<String, Long> usedBytesOnTiers,
        List<Long> removedBlocks,
        Map<BlockStoreLocation, List<Long>> addedBlocks,
        Map<String, List<String>> lostStorage,
        List<Metric> metrics) throws IOException {
      if (mReturnRegisterCommand) {
        return Command.newBuilder().setCommandType(CommandType.Register).build();
      }
      return Command.newBuilder().setCommandType(CommandType.Nothing).build();
    }

    @Override
    public void acquireRegisterLeaseWithBackoff(
        long workerId, int estimatedBlockCount, RetryPolicy retry)
        throws IOException, FailedToAcquireRegisterLeaseException {
    }
  }

  public BlockMasterClientPool mClientPool = new BlockMasterClientPool() {
    @Override
    public BlockMasterClient acquire() {
      return TestBlockMasterClient.INSTANCE;
    }

    @Override
    public void release(BlockMasterClient resource) {
    }
  };

  private BlockWorker getMockedBlockWorker() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    BlockMetadataManager metadataManager =
        TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());

    BlockWorker blockWorker = Mockito.mock(BlockWorker.class);
    Mockito.when(blockWorker.getStoreMetaFull())
        .thenReturn(metadataManager.getBlockStoreMetaFull());
    Mockito.when(blockWorker.getStoreMeta())
        .thenReturn(metadataManager.getBlockStoreMetaFull());
    Mockito.when(blockWorker.getReport())
        .thenReturn(new BlockHeartbeatReport(Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyMap()));
    return blockWorker;
  }
}
