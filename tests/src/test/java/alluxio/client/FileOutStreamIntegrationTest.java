/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client;

import java.io.IOException;

import org.junit.Test;

import alluxio.Constants;
import alluxio.TachyonURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.TachyonException;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream}.
 */
public final class FileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  // TODO(binfan): Run tests with local writes enabled and disabled.
  /**
   * Test {@link FileOutStream#write(int)}.
   */
  @Test
  public void writeTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        writeTest1Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode()), k, op);
      }
    }
  }

  private void writeTest1Util(TachyonURI filePath, int len, CreateFileOptions op)
      throws IOException, TachyonException {
    FileOutStream os = mFileSystem.createFile(filePath, op);
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len);
  }

  /**
   * Test {@link FileOutStream#write(byte[])}.
   */
  @Test
  public void writeTest2() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        writeTest2Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode()), k, op);
      }
    }
  }

  private void writeTest2Util(TachyonURI filePath, int len, CreateFileOptions op)
      throws IOException, TachyonException {
    FileOutStream os = mFileSystem.createFile(filePath, op);
    os.write(BufferUtils.getIncreasingByteArray(len));
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len);
  }

  /**
   * Test {@link FileOutStream#write(byte[], int, int)}.
   */
  @Test
  public void writeTest3() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFileOptions op : getOptionSet()) {
        writeTest3Util(new TachyonURI(uniqPath + "/file_" + k + "_" + op.hashCode()), k, op);
      }
    }
  }

  private void writeTest3Util(TachyonURI filePath, int len, CreateFileOptions op)
      throws IOException, TachyonException {
    FileOutStream os = mFileSystem.createFile(filePath, op);
    os.write(BufferUtils.getIncreasingByteArray(0, len / 2), 0, len / 2);
    os.write(BufferUtils.getIncreasingByteArray(len / 2, len / 2), 0, len / 2);
    os.close();
    checkWrite(filePath, op.getUnderStorageType(), len, len / 2 * 2);
  }

  /**
   * Test writing to a file and specify the location to be localhost.
   *
   * @throws IOException if file can not be opened successfully
   */
  @Test
  public void writeSpecifyLocalTest() throws IOException, TachyonException {
    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath, mWriteLocal);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, mWriteLocal.getUnderStorageType(), length, length);
  }

  /**
   * Test writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the sessionId doesn't
   * change. Tracks [TACHYON-171].
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void longWriteTest() throws IOException, InterruptedException, TachyonException {
    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath, mWriteUnderStore);
    os.write((byte) 0);
    Thread.sleep(mTestConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS) * 2);
    os.write((byte) 1);
    os.close();
    checkWrite(filePath, mWriteUnderStore.getUnderStorageType(), length, length);
  }

  /**
   * Tests if out-of-order writes are possible. Writes could be out-of-order when the following are
   * both true: - a "large" write (over half the internal buffer size) follows a smaller write. -
   * the "large" write does not cause the internal buffer to overflow.
   *
   * @throws IOException
   */
  @Test
  public void outOfOrderWriteTest() throws IOException, TachyonException {
    TachyonURI filePath = new TachyonURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, mWriteTachyon);

    // Write something small, so it is written into the buffer, and not directly to the file.
    os.write((byte) 0);

    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;

    // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
    os.write(BufferUtils.getIncreasingByteArray(1, length));
    os.close();

    checkWrite(filePath, mWriteTachyon.getUnderStorageType(), length + 1, length + 1);
  }
}
