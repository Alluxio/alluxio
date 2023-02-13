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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.Sessions;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.JournalType;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link DefaultBlockWorker}.
 */
public class AllMasterRegistrationBlockWorkerTest extends DefaultBlockWorkerTestBase {
  @Override
  public void before() throws Exception {
    mConfigurationRule.set(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT, "5s");
    mConfigurationRule.set(PropertyKey.TEST_MODE, true);
    mConfigurationRule.set(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS, true);
    mConfigurationRule.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED);
    mConfigurationRule.set(PropertyKey.MASTER_RPC_ADDRESSES,
        "localhost:19998,localhost:19988,localhost:19978");
    super.before();

    when(mFileSystemMasterClient.getRemoteSockAddress())
        .thenReturn(InetSocketAddress.createUnresolved("localhost", 19998));

    mBlockWorker = new AllMasterRegistrationBlockWorker(
        mBlockMasterClientPool, mFileSystemMasterClient,
        mock(Sessions.class), mBlockStore, new AtomicReference<>(INVALID_WORKER_ID));
    BlockSyncMasterGroup.setBlockMasterClientFactory(
        new BlockSyncMasterGroup.BlockMasterClientFactory() {
          @Override
          BlockMasterClient create(InetSocketAddress address) {
            return mBlockMasterClient;
          }
        });
  }

  @Test
  public void workerMasterRegistrationFailed() throws IOException {
    doThrow(new RuntimeException("error")).when(mBlockMasterClient).registerWithStream(
        anyLong(), any(), any(), any(), any(), any(), any());
    Exception e = assertThrows(Exception.class, () -> mBlockWorker.start(WORKER_ADDRESS));
    assertTrue(e.getMessage().contains("Fatal error: Failed to register with primary master"));
  }

  @Test
  public void workerMasterRegistration() throws IOException {
    mBlockWorker.start(WORKER_ADDRESS);
  }

  // TODO(elega) add a test to confirm the worker can start when the registration to a standby fails
}
