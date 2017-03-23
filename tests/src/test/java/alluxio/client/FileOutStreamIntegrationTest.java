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

import static alluxio.client.AbstractFileOutStreamIntegrationTest.BUFFER_BYTES;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
      Thread.sleep(Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS) * 2);
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
   * Tests when there are multiple writers writing to the same file concurrently with short circuit
   * off. Set the block size to be much bigger than the file buffer size so that we can excise
   * more code.
   */
  /*
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "false",
          PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "10240"})
  public void concurrentRemoteWrite() throws Exception {
    final AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length =
        (Configuration.getInt(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT) * 2) / BUFFER_BYTES
            * BUFFER_BYTES;

    final AtomicInteger count = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(20);
    for (int i = 0; i < 1; ++i) {
      service.submit(new Runnable() {
        @Override
        public void run() {
          FileOutStream os;
          if (i == 0) {

          }
          try (FileOutStream os = mFileSystem
              .createFile(filePath, CreateFileOptions.defaults().setWriteType(mWriteType))) {
            for (int i = 0; i < length / BUFFER_BYTES; ++i) {
              os.write(BufferUtils.getIncreasingByteArray(i * BUFFER_BYTES, BUFFER_BYTES));
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
            // Ignore.
          }
          try {
            if (mWriteType.getAlluxioStorageType().isStore()) {
              checkFileInAlluxio(filePath, length);
            }
            if (mWriteType.getUnderStorageType().isSyncPersist()) {
              checkFileInUnderStorage(filePath, length);
            }
            count.incrementAndGet();
          } catch (Exception e) {
            // Ignore.
          }
        }
      });
    }
    service.shutdown();
    service.awaitTermination(Constants.MINUTE_MS, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, count.get());
  }
  */
}
