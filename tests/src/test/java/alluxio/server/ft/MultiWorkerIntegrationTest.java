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

package alluxio.server.ft;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
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
import java.util.stream.StreamSupport;

/**
 * Tests a cluster containing multiple workers.
 */
public final class MultiWorkerIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_WORKERS = 4;
  private static final int WORKER_MEMORY_SIZE_BYTES = Constants.MB;
  private static final int BLOCK_SIZE_BYTES = WORKER_MEMORY_SIZE_BYTES / 2;

  public static class FindFirstBlockLocationPolicy implements BlockLocationPolicy {
    // Set this prior to sending the create request to FSM.
    private static WorkerNetAddress sWorkerAddress;

    public FindFirstBlockLocationPolicy(AlluxioConfiguration conf) {
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
      return StreamSupport.stream(options.getBlockWorkerInfos().spliterator(), false)
          .filter(x -> x.getNetAddress().equals(sWorkerAddress)).findFirst().get()
          .getNetAddress();
    }
  }

  @Rule
  public LocalAlluxioClusterResource mResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, WORKER_MEMORY_SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BLOCK_SIZE_BYTES)
          .setNumWorkers(NUM_WORKERS)
          .build();

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_BLOCK_WRITE_LOCATION_POLICY,
      "alluxio.client.block.policy.RoundRobinPolicy",
      })
  public void writeLargeFile() throws Exception {
    int fileSize = NUM_WORKERS * WORKER_MEMORY_SIZE_BYTES;
    AlluxioURI file = new AlluxioURI("/test");

    FileSystem fs = mResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, file.getPath(), fileSize,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
    URIStatus status = fs.getStatus(file);
    assertEquals(100, status.getInAlluxioPercentage());
    try (FileInStream inStream = fs.openFile(file)) {
      assertEquals(fileSize, IOUtils.toByteArray(inStream).length);
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED,
      "false", PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "16MB",
      PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "1s",
      PropertyKey.Name.WORKER_RAMDISK_SIZE, "1GB"})
  public void readRecoverFromLostWorker() throws Exception {
    int offset = 17 * Constants.MB;
    int length = 33 * Constants.MB;
    int total = offset + length;
    // creates a test file on one worker
    AlluxioURI filePath = new AlluxioURI("/test");
    createFileOnWorker(total, filePath, mResource.get().getWorkerAddress());
    FileSystem fs = mResource.get().getClient();
    try (FileInStream in = fs.openFile(filePath, OpenFilePOptions.getDefaultInstance())) {
      byte[] buf = new byte[total];
      int size = in.read(buf, 0, offset);
      replicateFileBlocks(filePath);
      mResource.get().getWorkerProcess().stop();
      size += in.read(buf, offset, length);

      Assert.assertEquals(total, size);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(offset, size, buf));
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED,
      "false", PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "4MB",
      PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "1s",
      PropertyKey.Name.WORKER_RAMDISK_SIZE, "1GB"})
  public void readOneRecoverFromLostWorker() throws Exception {
    int offset = 1 * Constants.MB;
    int length = 5 * Constants.MB;
    int total = offset + length;
    // creates a test file on one worker
    AlluxioURI filePath = new AlluxioURI("/test");
    FileSystem fs = mResource.get().getClient();
    createFileOnWorker(total, filePath, mResource.get().getWorkerAddress());
    try (FileInStream in = fs.openFile(filePath, OpenFilePOptions.getDefaultInstance())) {
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

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED,
      "false", PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "4MB",
      PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB",
      PropertyKey.Name.USER_BLOCK_READ_RETRY_MAX_DURATION, "1s",
      PropertyKey.Name.WORKER_RAMDISK_SIZE, "1GB"})
  public void positionReadRecoverFromLostWorker() throws Exception {
    int offset = 1 * Constants.MB;
    int length = 7 * Constants.MB;
    int total = offset + length;
    // creates a test file on one worker
    AlluxioURI filePath = new AlluxioURI("/test");
    FileSystem fs = mResource.get().getClient();
    createFileOnWorker(total, filePath, mResource.get().getWorkerAddress());
    try (FileInStream in = fs.openFile(filePath, OpenFilePOptions.getDefaultInstance())) {
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
    FindFirstBlockLocationPolicy.sWorkerAddress = address;
    String previousPolicy = ServerConfiguration.get(PropertyKey.USER_BLOCK_WRITE_LOCATION_POLICY);
    // This only works because the client instance hasn't been created yet.
    ServerConfiguration.set(PropertyKey.USER_BLOCK_WRITE_LOCATION_POLICY,
        FindFirstBlockLocationPolicy.class.getName());
    FileSystemTestUtils.createByteFile(mResource.get().getClient(), filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build(),
        total);
    ServerConfiguration.set(PropertyKey.USER_BLOCK_WRITE_LOCATION_POLICY, previousPolicy);
  }

  private void replicateFileBlocks(AlluxioURI filePath) throws Exception {
    FileSystemContext fsContext = FileSystemContext.create(ServerConfiguration.global());
    AlluxioBlockStore store = AlluxioBlockStore.create(fsContext);
    URIStatus status =  mResource.get().getClient().getStatus(filePath);
    List<FileBlockInfo> blocks = status.getFileBlockInfos();
    List<BlockWorkerInfo> workers = fsContext.getCachedWorkers();

    for (FileBlockInfo block : blocks) {
      BlockInfo blockInfo = block.getBlockInfo();
      WorkerNetAddress src = blockInfo.getLocations().get(0).getWorkerAddress();
      WorkerNetAddress dest = workers.stream()
          .filter(candidate -> !candidate.getNetAddress().equals(src))
          .findFirst()
          .get()
          .getNetAddress();
      try (OutputStream outStream = store.getOutStream(blockInfo.getBlockId(),
          blockInfo.getLength(), dest, OutStreamOptions.defaults(fsContext.getClientContext())
              .setBlockSizeBytes(8 * Constants.MB).setWriteType(WriteType.MUST_CACHE))) {
        try (InputStream inStream = store.getInStream(blockInfo.getBlockId(),
            new InStreamOptions(status, ServerConfiguration.global()))) {
          ByteStreams.copy(inStream, outStream);
        }
      }
    }
  }
}
