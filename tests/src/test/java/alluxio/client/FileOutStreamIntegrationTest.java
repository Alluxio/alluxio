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
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Test;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream}.
 */
public final class FileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  // TODO(binfan): Run tests with local writes enabled and disabled.
  /**
   * Tests {@link FileOutStream#write(int)}.
   */
  @Test
  public void writeTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        writeTest1Util(new AlluxioURI(uniqPath + "/file_" + k + "_" + op.hashCode()), k, op);
      }
    }
  }

  private void writeTest1Util(AlluxioURI filePath, int len, CreateFileOptions op) throws Exception {
    FileOutStream os = mFileSystem.createFile(filePath, op);
    for (int k = 0; k < len; k++) {
      os.write((byte) k);
    }
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len);
  }

  /**
   * Tests {@link FileOutStream#write(byte[])}.
   */
  @Test
  public void writeTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        writeTest2Util(new AlluxioURI(uniqPath + "/file_" + k + "_" + op.hashCode()), k, op);
      }
    }
  }

  private void writeTest2Util(AlluxioURI filePath, int len, CreateFileOptions op) throws Exception {
    FileOutStream os = mFileSystem.createFile(filePath, op);
    os.write(BufferUtils.getIncreasingByteArray(len));
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len);
  }

  /**
   * Tests {@link FileOutStream#write(byte[], int, int)}.
   */
  @Test
  public void writeTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        writeTest3Util(new AlluxioURI(uniqPath + "/file_" + k + "_" + op.hashCode()), k, op);
      }
    }
  }

  private void writeTest3Util(AlluxioURI filePath, int len, CreateFileOptions op) throws Exception {
    FileOutStream os = mFileSystem.createFile(filePath, op);
    os.write(BufferUtils.getIncreasingByteArray(0, len / 2), 0, len / 2);
    os.write(BufferUtils.getIncreasingByteArray(len / 2, len / 2), 0, len / 2);
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len / 2 * 2);
  }

  /**
   * Tests writing to a file and specify the location to be localhost.
   */
  @Test
  public void writeSpecifyLocal() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH)
            .setLocationPolicy(new LocalFirstPolicy()));
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, UnderStorageType.SYNC_PERSIST, length, length);
  }

  /**
   * Tests writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the sessionId
   * doesn't change. Tracks [ALLUXIO-171].
   */
  @Test
  public void longWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath,
            CreateFileOptions.defaults().setWriteType(WriteType.THROUGH));
    os.write((byte) 0);
    Thread.sleep(Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS) * 2);
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, UnderStorageType.SYNC_PERSIST, length, length);
  }

  /**
   * Tests if out-of-order writes are possible. Writes could be out-of-order when the following are
   * both true: - a "large" write (over half the internal buffer size) follows a smaller write. -
   * the "large" write does not cause the internal buffer to overflow.
   */
  @Test
  public void outOfOrderWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath,
            CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE));

    // Write something small, so it is written into the buffer, and not directly to the file.
    os.write((byte) 0);

    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;

    // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
    os.write(BufferUtils.getIncreasingByteArray(1, length));
    os.close();

    checkWrite(filePath, UnderStorageType.NO_PERSIST, length + 1, length + 1);
  }
}
