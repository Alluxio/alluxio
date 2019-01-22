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

package alluxio.worker.grpc;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.grpc.RequestType;
import alluxio.grpc.WriteRequest;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.io.BlockWriter;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

public class UfsFallbackBlockWriteHandlerTest extends AbstractWriteHandlerTest {
  private static final long TEST_SESSION_ID = 123L;
  private static final long TEST_WORKER_ID = 456L;
  private static final int PARTIAL_WRITTEN = 512;

  private OutputStream mOutputStream;
  private BlockWorker mBlockWorker;
  private BlockStore mBlockStore;
  /** The file used to hold the data written by the test. */
  private File mFile;
  private long mPartialChecksum;

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, String>() {
        {
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              AlluxioTestDirectory.createTemporaryDirectory(
                  "UfsFallbackBlockWriteHandlerTest-RootUfs")
                  .getAbsolutePath());
          put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, AlluxioTestDirectory
              .createTemporaryDirectory("UfsFallbackBlockWriteHandlerTest-WorkerDataFolder")
              .getAbsolutePath());
          put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "1");
        }
      });

  @Before
  public void before() throws Exception {
    mFile = mTestFolder.newFile();
    mOutputStream = new FileOutputStream(mFile);
    mBlockStore = new TieredBlockStore();
    mBlockWorker = Mockito.mock(BlockWorker.class);
    Mockito.when(mBlockWorker.getBlockStore()).thenReturn(mBlockStore);
    Mockito.when(mBlockWorker.getWorkerId()).thenReturn(
        new AtomicReference<>(TEST_WORKER_ID));
    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    UfsManager.UfsClient ufsClient = new UfsManager.UfsClient(() -> mockUfs, AlluxioURI.EMPTY_URI);
    Mockito.when(ufsManager.get(Mockito.anyLong())).thenReturn(ufsClient);
    Mockito.when(mockUfs.create(Mockito.anyString(), Mockito.any(CreateOptions.class)))
        .thenReturn(mOutputStream)
        .thenReturn(new FileOutputStream(mFile, true));

    mResponseObserver = Mockito.mock(StreamObserver.class);
    mWriteHandler = new UfsFallbackBlockWriteHandler(mBlockWorker, ufsManager, mResponseObserver);

    // create a partial block in block store first
    mBlockStore.createBlock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockStoreLocation.anyDirInTier("MEM"), CHUNK_SIZE);
    BlockWriter writer = mBlockStore.getBlockWriter(TEST_SESSION_ID, TEST_BLOCK_ID);
    DataBuffer buffer = newDataBuffer(PARTIAL_WRITTEN);
    mPartialChecksum = getChecksum(buffer);
    writer.append((ByteBuf) buffer.getNettyOutput());
    writer.close();
  }

  @After
  public void after() throws Exception {
    mOutputStream.close();
  }

  @Test
  public void noTempBlockFound() throws Exception {
    // remove the block partially created
    mBlockStore.abortBlock(TEST_SESSION_ID, TEST_BLOCK_ID);
    mWriteHandler.write(newFallbackInitRequest(PARTIAL_WRITTEN));
    checkErrorCode(mResponseObserver, Status.Code.NOT_FOUND);
  }

  @Test
  public void tempBlockWritten() throws Exception {
    DataBuffer buffer = newDataBuffer(CHUNK_SIZE);
    long checksum = mPartialChecksum + getChecksum(buffer);
    mWriteHandler.write(newFallbackInitRequest(PARTIAL_WRITTEN));
    mWriteHandler.write(newWriteRequest(buffer));
    mWriteHandler.onCompleted();
    checkComplete(mResponseObserver);
    checkWriteData(checksum, PARTIAL_WRITTEN + CHUNK_SIZE);
  }

  protected WriteRequest newFallbackInitRequest(long bytesInBlockStore) {
    Protocol.CreateUfsBlockOptions createUfsBlockOptions =
        newWriteRequestCommand(0).getCommand().getCreateUfsBlockOptions().toBuilder()
            .setBytesInBlockStore(bytesInBlockStore)
            .build();
    WriteRequest request = super.newWriteRequestCommand(0).toBuilder().setCommand(
        super.newWriteRequestCommand(0).getCommand().toBuilder()
        .setCreateUfsBlockOptions(createUfsBlockOptions)).build();
    return request;
  }

  @Override
  protected WriteRequest newWriteRequestCommand(long offset) {
    Protocol.CreateUfsBlockOptions createUfsBlockOptions =
        Protocol.CreateUfsBlockOptions.newBuilder().setMountId(TEST_MOUNT_ID).setFallback(true)
            .build();
    return super.newWriteRequestCommand(offset).toBuilder().setCommand(
        super.newWriteRequestCommand(offset).getCommand().toBuilder()
        .setCreateUfsBlockOptions(createUfsBlockOptions)).build();
  }

  @Override
  protected RequestType getWriteRequestType() {
    return RequestType.UFS_FALLBACK_BLOCK;
  }

  @Override
  protected InputStream getWriteDataStream() throws IOException {
    return new FileInputStream(mFile);
  }
}
