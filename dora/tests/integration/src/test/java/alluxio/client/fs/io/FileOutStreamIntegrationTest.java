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

package alluxio.client.fs.io;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerInfo;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream}, parameterized by the write
 * types.
 */
@RunWith(Parameterized.class)
public final class FileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  // TODO(binfan): Run tests with local writes enabled and disabled.

  @Parameters
  public static Object[] dataFileInStreamIntegrationTest() {
    return new Object[] {
        WriteType.CACHE_THROUGH,
        WriteType.THROUGH,
    };
  }

  @Parameter
  public WriteType mWriteType;

  /**
   * Tests {@link FileOutStream#write(int)}.
   */
  @Test
  public void writeBytes() throws Exception {
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFilePOptions op = CreateFilePOptions.newBuilder().setWriteType(mWriteType.toProto())
          .setRecursive(true).build();
      AlluxioURI filePath = new AlluxioURI(mRoot).join(
          String.format("test_writeBytes_file_len_%d", len));
      writeIncreasingBytesToFile(filePath, len, op);
      if (mWriteType.getAlluxioStorageType().isStore()) {
        checkFileInAlluxio(filePath, len);
      }
      if (mWriteType.getUnderStorageType().isSyncPersist()) {
        checkFileInUnderStorage(filePath, len);
      }
    }
  }

  /**
   * Tests {@link FileOutStream#write(int)}.
   */
  @Test
  public void writeByte() throws Exception {
    CreateFilePOptions options = CreateFilePOptions.newBuilder()
        .setWriteType(mWriteType.toProto()).setRecursive(true).build();
    AlluxioURI filePath = new AlluxioURI(mRoot).join("test_writeByte");
    writeOneIntegerToFile(filePath, 0, options);
    if (mWriteType.getAlluxioStorageType().isStore()) {
      checkFileInAlluxio(filePath, 1);
    }
    if (mWriteType.getUnderStorageType().isSyncPersist()) {
      checkFileInUnderStorage(filePath, 1);
    }
  }

  /**
   * Tests {@link FileOutStream#write(int)}.
   */
  @Test
  public void writeInNonExistDirectory() throws Exception {
    CreateFilePOptions op = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build();
    AlluxioURI filePath = new AlluxioURI(mRoot).join("test_writeInNonExistDirectory");
    AlluxioURI parentPath = new AlluxioURI(mRoot);

    // create a directory without a backing directory in UFS
    mFileSystem.createDirectory(parentPath, CreateDirectoryPOptions.newBuilder()
        .setRecursive(true)
        .setWriteType(WritePType.CACHE_THROUGH)
        .build());
    URIStatus status = mFileSystem.getStatus(parentPath);
    String checkpointPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(checkpointPath,
        Configuration.global());
    ufs.deleteDirectory(checkpointPath);

    // write a file to a directory exists in Alluxio but not in UFS
    writeIncreasingBytesToFile(filePath, MIN_LEN, op);
    checkFileInAlluxio(filePath, MIN_LEN);
    checkFileInUnderStorage(filePath, MIN_LEN);
  }

  /**
   * Tests {@link FileOutStream#write(byte[])}.
   */
  @Test
  public void writeByteArray() throws Exception {
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFilePOptions op = CreateFilePOptions.newBuilder().setWriteType(mWriteType.toProto())
          .setRecursive(true).build();
      AlluxioURI filePath = new AlluxioURI(mRoot).join(
          String.format("test_writeByteArray_file_len_%d", len));
      writeIncreasingByteArrayToFile(filePath, len, op);
      if (mWriteType.getAlluxioStorageType().isStore()) {
        checkFileInAlluxio(filePath, len);
      }
      if (mWriteType.getUnderStorageType().isSyncPersist()) {
        checkFileInUnderStorage(filePath, len);
      }
    }
  }

  /**
   * Tests {@link FileOutStream#write(byte[], int, int)}.
   */
  @Test
  public void writeTwoByteArrays() throws Exception {
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFilePOptions op = CreateFilePOptions.newBuilder().setWriteType(mWriteType.toProto())
          .setRecursive(true).build();
      String uniqPath = PathUtils.uniqPath();
      AlluxioURI filePath = new AlluxioURI(mRoot).join(
          String.format("test_writeTwoByteArrays_file_len_%d", len));
      writeTwoIncreasingByteArraysToFile(filePath, len, op);
      if (mWriteType.getAlluxioStorageType().isStore()) {
        checkFileInAlluxio(filePath, len);
      }
      if (mWriteType.getUnderStorageType().isSyncPersist()) {
        checkFileInUnderStorage(filePath, len);
      }
    }
  }

  /**
   * Tests writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the sessionId
   * doesn't change. Tracks [ALLUXIO-171].
   */
  @Test
  public void longWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(mRoot).join("test_longWrite");
    final int length = 2;
    try (FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(mWriteType.toProto()).setRecursive(true).build())) {
      os.write((byte) 0);
      Thread.sleep(Constants.SECOND_MS * 2);
      os.write((byte) 1);
    }
    if (mWriteType.getAlluxioStorageType().isStore()) {
      checkFileInAlluxio(filePath, length);
    }
    if (mWriteType.getUnderStorageType().isSyncPersist()) {
      checkFileInUnderStorage(filePath, length);
    }
  }

  /**
   * Tests if out-of-order writes are possible. Writes could be out-of-order when the following are
   * both true: - a "large" write (over half the internal buffer size) follows a smaller write. -
   * the "large" write does not cause the internal buffer to overflow.
   */
  @Test
  public void outOfOrderWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(mRoot).join("test_outOfOrderWrite");
    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;
    try (FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(mWriteType.toProto()).setRecursive(true).build())) {
      // Write something small, so it is written into the buffer, and not directly to the file.
      os.write((byte) 0);
      // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
      os.write(BufferUtils.getIncreasingByteArray(1, length));
    }
    if (mWriteType.getAlluxioStorageType().isStore()) {
      checkFileInAlluxio(filePath, length + 1);
    }
    if (mWriteType.getUnderStorageType().isSyncPersist()) {
      checkFileInUnderStorage(filePath, length + 1);
    }
  }

  /**
   * Tests canceling after multiple blocks have been written correctly cleans up temporary worker
   * resources.
   */
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.MASTER_LOST_WORKER_FILE_DETECTION_INTERVAL, "250ms",
          PropertyKey.Name.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "250ms"})
  @Test
  public void cancelWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(mRoot).join("test_cancelWrite");
    try (FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(mWriteType.toProto()).setRecursive(true).build())) {
      os.write(BufferUtils.getIncreasingByteArray(0, BLOCK_SIZE_BYTES * 3 + 1));
      os.cancel();
    }
    long gracePeriod = Configuration
        .getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2;
    CommonUtils.sleepMs(gracePeriod);
    List<WorkerInfo> workers =
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
            .getMaster(FileSystemMaster.class).getFileSystemMasterView().getWorkerInfoList();
    for (WorkerInfo worker : workers) {
      Assert.assertEquals(0, worker.getUsedBytes());
    }
  }
}
