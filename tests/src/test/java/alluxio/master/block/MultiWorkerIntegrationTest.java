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

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.BaseIntegrationTest;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Tests a cluster containing multiple workers.
 */
public final class MultiWorkerIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_WORKERS = 4;
  private static final int WORKER_MEMORY_SIZE_BYTES = Constants.MB;
  private static final int BLOCK_SIZE_BYTES = WORKER_MEMORY_SIZE_BYTES / 2;

  @Rule
  public LocalAlluxioClusterResource mResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_MEMORY_SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BLOCK_SIZE_BYTES)
          .setNumWorkers(NUM_WORKERS)
          .build();

  @Test
  public void writeLargeFile() throws Exception {
    int fileSize = NUM_WORKERS * WORKER_MEMORY_SIZE_BYTES;
    AlluxioURI file = new AlluxioURI("/test");
    FileSystem fs = mResource.get().getClient();
    // Write a file large enough to fill all the memory of all the workers.
    FileSystemTestUtils.createByteFile(fs, file.getPath(), fileSize,
        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
            .setLocationPolicy(new RoundRobinPolicy()));
    URIStatus status = fs.getStatus(file);
    assertEquals(100, status.getInAlluxioPercentage());
    try (FileInStream inStream = fs.openFile(file)) {
      assertEquals(fileSize, IOUtils.toByteArray(inStream).length);
    }
  }

  @Test(timeout = 300000)
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED,
      "false", PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "16MB",
      PropertyKey.Name.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64KB",
      PropertyKey.Name.WORKER_MEMORY_SIZE, "1GB"})
  public void readRecoverFromLostWorker() throws Exception {
    int offset = 17 * Constants.MB;
    int length = 33 * Constants.MB;
    int total = offset + length;
    // creates a test file on one worker
    AlluxioURI filePath = new AlluxioURI("/test");
    createFileOnWorker(total, filePath, mResource.get().getWorkerAddress());
    FileSystem fs = mResource.get().getClient();
    try (FileInStream in = fs.openFile(filePath, OpenFileOptions.defaults())) {
      byte[] buf = new byte[total];
      int size = in.read(buf, 0, offset);
      replicateFileBlocks(filePath);
      mResource.get().getWorkerProcess().stop();
      size += in.read(buf, offset, length);

      Assert.assertEquals(total, size);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(offset, size, buf));
    }
  }

  @Test(timeout = 300000)
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED,
      "false", PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "2MB",
      PropertyKey.Name.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64KB",
      PropertyKey.Name.WORKER_MEMORY_SIZE, "1GB"})
  public void readOneRecoverFromLostWorker() throws Exception {
    int offset = 3 * Constants.MB ;
    int length = 3 * Constants.MB;
    int total = offset + length;
    // creates a test file on one worker
    AlluxioURI filePath = new AlluxioURI("/test");
    FileSystem fs = mResource.get().getClient();
    createFileOnWorker(total, filePath, mResource.get().getWorkerAddress());
    try (FileInStream in = fs.openFile(filePath, OpenFileOptions.defaults())) {
      byte[] buf = new byte[total];
      int size = in.read(buf, 0, offset);
      replicateFileBlocks(filePath);
      mResource.get().getWorkerProcess().stop();
      for (int i = 0; i < length; i++) {
        int result = in.read();
        Assert.assertEquals(result, (i + size) & 0xff);
      }
    }
  }

  @Test(timeout = 300000)
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED,
      "false", PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "16MB",
      PropertyKey.Name.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64KB",
      PropertyKey.Name.WORKER_MEMORY_SIZE, "1GB"})
  public void positionReadRecoverFromLostWorker() throws Exception {
    int offset = 17 * Constants.MB;
    int length = 33 * Constants.MB;
    int total = offset + length;
    // creates a test file on one worker
    AlluxioURI filePath = new AlluxioURI("/test");
    FileSystem fs = mResource.get().getClient();
    createFileOnWorker(total, filePath, mResource.get().getWorkerAddress());
    try (FileInStream in = fs.openFile(filePath, OpenFileOptions.defaults())) {
      byte[] buf = new byte[length];
      replicateFileBlocks(filePath);
      mResource.get().getWorkerProcess().stop();
      int size = in.positionedRead(offset, buf, 0, length);

      Assert.assertEquals(length, size);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(offset, size, buf));
    }
  }

  private void createFileOnWorker(int total, AlluxioURI filePath, WorkerNetAddress address)
      throws IOException {
    FileSystemTestUtils.createByteFile(mResource.get().getClient(), filePath,
        CreateFileOptions.defaults()
            .setWriteType(WriteType.MUST_CACHE)
            .setLocationPolicy((workerInfoList, blockSizeBytes) ->
                StreamSupport.stream(workerInfoList.spliterator(), false)
                    .filter(x -> x.getNetAddress().equals(address))
                    .findFirst()
                    .get()
                    .getNetAddress()), total);
  }

  private void replicateFileBlocks(AlluxioURI filePath) throws Exception {
    AlluxioBlockStore store = AlluxioBlockStore.create();
    URIStatus status =  mResource.get().getClient().getStatus(filePath);
    List<FileBlockInfo> blocks = status.getFileBlockInfos();
    List<BlockWorkerInfo> workers = store.getAllWorkers();

    for (FileBlockInfo block : blocks) {
      BlockInfo blockInfo = block.getBlockInfo();
      WorkerNetAddress src = blockInfo.getLocations().get(0).getWorkerAddress();
      WorkerNetAddress dest = workers.stream()
          .filter(candidate -> !candidate.getNetAddress().equals(src))
          .findFirst()
          .get()
          .getNetAddress();
      try (OutputStream outStream = store.getOutStream(blockInfo.getBlockId(),
          blockInfo.getLength(), dest, OutStreamOptions.defaults()
              .setBlockSizeBytes(8 * Constants.MB).setWriteType(WriteType.MUST_CACHE))) {
        try (InputStream inStream = store.getInStream(blockInfo.getBlockId(),
            new InStreamOptions(status))) {
          ByteStreams.copy(inStream, outStream);
        }
      }
    }
  }
}
