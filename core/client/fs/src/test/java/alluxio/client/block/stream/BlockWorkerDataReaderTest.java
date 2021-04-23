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

package alluxio.client.block.stream;

import static org.mockito.ArgumentMatchers.any;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockReadRequest;
import alluxio.wire.FileInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.MockBlockReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Random;

/**
 * Unit tests for {@link BlockWorkerDataReader}.
 */
public class BlockWorkerDataReaderTest {
  private static final long BLOCK_ID = 1L;
  private static final int CHUNK_SIZE = 128;
  private static final Random RANDOM = new Random();

  private BlockWorker mBlockWorker;
  private BlockWorkerDataReader.Factory mDataReaderFactory;

  @Before
  public void before() throws Exception {
    mBlockWorker = Mockito.mock(BlockWorker.class);
    FileSystemContext context = Mockito.mock(FileSystemContext.class);
    Mockito.when(context.getProcessLocalWorker()).thenReturn(mBlockWorker);

    URIStatus dummyStatus = new URIStatus(new FileInfo()
        .setBlockIds(Collections.singletonList(BLOCK_ID)));
    AlluxioConfiguration conf
        = new InstancedConfiguration(ConfigurationUtils.defaults());
    InStreamOptions options = new InStreamOptions(dummyStatus,
        FileSystemOptions.openFileDefaults(conf), conf);
    mDataReaderFactory = new BlockWorkerDataReader.Factory(context, BLOCK_ID, CHUNK_SIZE, options);
  }

  @Test
  public void createAndClose() throws Exception {
    byte[] bytes = new byte[128];
    RANDOM.nextBytes(bytes);

    MockBlockReader blockReader = new MockBlockReader(bytes);
    Mockito.when(mBlockWorker.createBlockReader(any(BlockReadRequest.class)))
        .thenReturn(blockReader);
    DataReader dataReader = mDataReaderFactory.create(1, 100);

    MockBlockReader blockReaderTwo = new MockBlockReader(bytes);
    Mockito.when(mBlockWorker.createBlockReader(any(BlockReadRequest.class)))
        .thenReturn(blockReaderTwo);
    DataReader dataReaderTwo = mDataReaderFactory.create(10, 30);

    Assert.assertFalse(blockReader.isClosed());
    Assert.assertFalse(blockReaderTwo.isClosed());
    dataReaderTwo.close();
    Assert.assertFalse(blockReader.isClosed());
    Assert.assertTrue(blockReaderTwo.isClosed());
    dataReader.close();
    Assert.assertTrue(blockReader.isClosed());
  }

  @Test
  public void readChunkFullFile() throws Exception {
    int len = CHUNK_SIZE * 2;
    byte[] bytes = BufferUtils.getIncreasingByteArray(len);
    MockBlockReader blockReader = new MockBlockReader(bytes);
    Mockito.when(mBlockWorker.createBlockReader(any(BlockReadRequest.class)))
        .thenReturn(blockReader);
    DataReader dataReader = mDataReaderFactory.create(0, len);
    validateBuffer(dataReader.readChunk(), 0, CHUNK_SIZE);
    Assert.assertEquals(CHUNK_SIZE, dataReader.pos());
    validateBuffer(dataReader.readChunk(), CHUNK_SIZE, len);
    Assert.assertEquals(len, dataReader.pos());
    dataReader.close();
  }

  @Test
  public void readChunkPartial() throws Exception {
    int len = CHUNK_SIZE * 5;
    byte[] bytes = BufferUtils.getIncreasingByteArray(len);
    MockBlockReader blockReader = new MockBlockReader(bytes);
    Mockito.when(mBlockWorker.createBlockReader(any(BlockReadRequest.class)))
        .thenReturn(blockReader);
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
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(start, len, bytes));
  }
}
