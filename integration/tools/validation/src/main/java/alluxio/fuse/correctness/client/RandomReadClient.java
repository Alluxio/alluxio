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

package alluxio.fuse.correctness.client;

import alluxio.fuse.correctness.Constants;
import alluxio.fuse.correctness.Utils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This class is a client verifying the correctness of sequential read of AlluxioFuse.
 */
public class RandomReadClient implements Runnable {
  private final String mLocalFilePath;
  private final String mFuseFilePath;
  private final int mBufferSize;

  /**
   * Creates an instance of {@link RandomReadClient}.
   *
   * @param localFilePath
   * @param fuseFilePath
   * @param bufferSize
   */
  public RandomReadClient(String localFilePath, String fuseFilePath, int bufferSize) {
    mLocalFilePath = Preconditions.checkNotNull(localFilePath);
    mFuseFilePath = Preconditions.checkNotNull(fuseFilePath);
    mBufferSize = bufferSize;
  }

  @Override
  public void run() {
    try (RandomAccessFile localRandomFile = new RandomAccessFile(mLocalFilePath, "r");
         RandomAccessFile fuseRandomFile = new RandomAccessFile(mFuseFilePath, "r")) {
      final byte[] localFileBuffer = new byte[mBufferSize];
      final byte[] fuseFileBuffer = new byte[mBufferSize];
      for (int iteration = 0; iteration < 50000; iteration++) {
        long offset = Utils.nextRandomLong(localRandomFile.length());
        localRandomFile.seek(offset);
        fuseRandomFile.seek(offset);
        int localBytesRead = localRandomFile.read(localFileBuffer);
        int fuseBytesRead = fuseRandomFile.read(fuseFileBuffer);
        if (!Utils.isDataCorrect(
            localFileBuffer, fuseFileBuffer, localBytesRead, fuseBytesRead)) {
          System.out.println(String.format(
              Constants.DATA_INCONSISTENCY_FORMAT, Constants.RANDOM_READ, mBufferSize));
        }
        if (Thread.interrupted()) {
          return;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Some thread caught IOException. Test is stopped.", e);
    }
  }
}
