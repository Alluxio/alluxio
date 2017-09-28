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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.master.file.FileSystemMaster;
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
  public static Object[] data() {
    return new Object[] {
        WriteType.ASYNC_THROUGH,
        WriteType.CACHE_THROUGH,
        WriteType.MUST_CACHE,
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
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(mWriteType);
      AlluxioURI filePath =
          new AlluxioURI(PathUtils.concatPath(uniqPath, "file_" + len + "_" + mWriteType));
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
   * Tests {@link FileOutStream#write(byte[])}.
   */
  @Test
  public void writeByteArray() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(mWriteType);
      AlluxioURI filePath =
          new AlluxioURI(PathUtils.concatPath(uniqPath, "file_" + len + "_" + mWriteType));
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
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(mWriteType);
      AlluxioURI filePath =
          new AlluxioURI(PathUtils.concatPath(uniqPath, "file_" + len + "_" + mWriteType));
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
   * Tests writing to a file and specify the location to be localhost.
   */
  @Test
  public void writeSpecifyLocal() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    try (FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(mWriteType)
            .setLocationPolicy(new LocalFirstPolicy()))) {
      os.write((byte) 0);
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
   * Tests writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the sessionId
   * doesn't change. Tracks [ALLUXIO-171].
   */
  @Test
  public void longWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    try (FileOutStream os =
        mFileSystem.createFile(filePath, CreateFileOptions.defaults().setWriteType(mWriteType))) {
      os.write((byte) 0);
      Thread.sleep((int) Configuration.getMs(PropertyKey.USER_HEARTBEAT_INTERVAL_MS) * 2);
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
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;
    try (FileOutStream os =
        mFileSystem.createFile(filePath, CreateFileOptions.defaults().setWriteType(mWriteType))) {
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
  @Test
  public void cancelWrite() throws Exception {
    AlluxioURI path = new AlluxioURI(PathUtils.uniqPath());
    try (FileOutStream os =
        mFileSystem.createFile(path, CreateFileOptions.defaults().setWriteType(mWriteType))) {
      os.write(BufferUtils.getIncreasingByteArray(0, BLOCK_SIZE_BYTES * 3 + 1));
      os.cancel();
    }
    long gracePeriod = Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS) * 2;
    CommonUtils.sleepMs(gracePeriod);
    List<WorkerInfo> workers =
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
            .getMaster(FileSystemMaster.class).getFileSystemMasterView().getWorkerInfoList();
    for (WorkerInfo worker : workers) {
      Assert.assertEquals(0, worker.getUsedBytes());
    }
  }
}
