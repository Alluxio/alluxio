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

package alluxio.worker.block.stream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.block.stream.BlockWorkerDataReader;
import alluxio.client.block.stream.DataReader;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.underfs.UfsManager;
import alluxio.util.FileSystemOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Unit tests for {@link BlockWorkerDataReader}.
 */
public class BlockWorkerDataReaderTest {
  private static final long BLOCK_ID = 1L;
  private static final int CHUNK_SIZE = 128;
  private static final long SESSION_ID = 10L;
  private static final int LOCK_NUM = 5;

  private BlockWorker mBlockWorker;
  private BlockWorkerDataReader.Factory mDataReaderFactory;
  private BlockMasterClient mBlockMasterClient;
  private BlockMasterClientPool mBlockMasterClientPool;
  private TieredBlockStore mBlockStore;
  private FileSystemMasterClient mFileSystemMasterClient;
  private Sessions mSessions;
  private UfsManager mUfsManager;
  private String mMemDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();
  private InstancedConfiguration mConf = ServerConfiguration.global();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, String>()
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "1")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir)
          .put(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, String.valueOf(LOCK_NUM))
          .put(PropertyKey.WORKER_RPC_PORT, "0").build(), mConf);

  @Before
  public void before() throws Exception {
    mBlockMasterClient = mock(BlockMasterClient.class);
    mBlockMasterClientPool = spy(new BlockMasterClientPool());
    when(mBlockMasterClientPool.createNewResource()).thenReturn(mBlockMasterClient);
    mBlockStore = new TieredBlockStore();
    mFileSystemMasterClient = mock(FileSystemMasterClient.class);
    mSessions = mock(Sessions.class);
    mUfsManager = mock(UfsManager.class);
    mBlockWorker = new DefaultBlockWorker(mBlockMasterClientPool, mFileSystemMasterClient,
        mSessions, mBlockStore, mUfsManager);

    URIStatus dummyStatus =
        new URIStatus(new FileInfo().setBlockIds(Collections.singletonList(BLOCK_ID)));
    InStreamOptions options =
        new InStreamOptions(dummyStatus, FileSystemOptions.openFileDefaults(mConf), mConf);
    mDataReaderFactory =
        new BlockWorkerDataReader.Factory(mBlockWorker, BLOCK_ID, CHUNK_SIZE, options);
  }

  @Test
  public void createWithBlockNotExists() {
    assertThrows(IOException.class, () -> mDataReaderFactory.create(BLOCK_ID, 100));
  }

  @Test
  public void create() throws Exception {
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, 0, Constants.MEDIUM_MEM, 1);
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID, true);
    DataReader dataReader = mDataReaderFactory.create(100, 200);
    assertEquals(100, dataReader.pos());
  }

  // See https://github.com/Alluxio/alluxio/issues/13255
  @Test
  public void createAndCloseManyReader() throws Exception {
    for (int i = 0; i < LOCK_NUM * 10; i++) {
      long blockId = i;
      mBlockWorker.createBlock(SESSION_ID, blockId, 0, Constants.MEDIUM_MEM, 1);
      mBlockWorker.commitBlock(SESSION_ID, blockId, true);
      InStreamOptions inStreamOptions = new InStreamOptions(
          new URIStatus(new FileInfo().setBlockIds(Collections.singletonList(blockId))),
          FileSystemOptions.openFileDefaults(mConf), mConf);
      mDataReaderFactory =
          new BlockWorkerDataReader.Factory(mBlockWorker, blockId, CHUNK_SIZE, inStreamOptions);
      DataReader dataReader = mDataReaderFactory.create(0, 100);
      dataReader.close();
    }
  }

  @Test
  public void readChunkFullFile() throws Exception {
    int len = CHUNK_SIZE * 2;
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, 0, Constants.MEDIUM_MEM, 1);
    try (BlockWriter writer = mBlockWorker.createBlockWriter(SESSION_ID, BLOCK_ID)) {
      writer.append(BufferUtils.getIncreasingByteBuffer(len));
    }
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID, true);
    DataReader dataReader = mDataReaderFactory.create(0, len);
    validateBuffer(dataReader.readChunk(), 0, CHUNK_SIZE);
    assertEquals(CHUNK_SIZE, dataReader.pos());
    validateBuffer(dataReader.readChunk(), CHUNK_SIZE, CHUNK_SIZE);
    assertEquals(len, dataReader.pos());
    dataReader.close();
  }

  @Test
  public void readChunkPartial() throws Exception {
    int len = CHUNK_SIZE * 5;
    mBlockWorker.createBlock(SESSION_ID, BLOCK_ID, 0, Constants.MEDIUM_MEM, 1);
    try (BlockWriter writer = mBlockWorker.createBlockWriter(SESSION_ID, BLOCK_ID)) {
      writer.append(BufferUtils.getIncreasingByteBuffer(len));
    }
    mBlockWorker.commitBlock(SESSION_ID, BLOCK_ID, true);
    int start = len / 5 * 2;
    int end = len / 5 * 4;
    DataReader dataReader = mDataReaderFactory.create(start, end);
    for (int s = start; s < end; s += CHUNK_SIZE) {
      int currentLen = Math.min(CHUNK_SIZE, end - s);
      validateBuffer(dataReader.readChunk(), s, currentLen);
    }
  }

  private void validateBuffer(DataBuffer buffer, int start, int len) {
    byte[] bytes = new byte[buffer.readableBytes()];
    buffer.readBytes(bytes, 0, buffer.readableBytes());
    assertTrue(BufferUtils.equalIncreasingByteArray(start, len, bytes));
  }
}
