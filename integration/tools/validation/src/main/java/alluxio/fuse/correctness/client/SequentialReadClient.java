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

import java.io.FileInputStream;
import java.io.IOException;

/**
 * This class is a client verifying the correctness of sequential read of AlluxioFuse.
 */
public class SequentialReadClient implements Runnable {
  private final String mLocalFilePath;
  private final String mFuseFilePath;
  private final int mBufferSize;

  /**
   * Creates an instance of {@link SequentialReadClient}.
   *
   * @param localFilePath
   * @param fuseFilePath
   * @param bufferSize
   */
  public SequentialReadClient(String localFilePath, String fuseFilePath, int bufferSize) {
    mLocalFilePath = Preconditions.checkNotNull(localFilePath);
    mFuseFilePath = Preconditions.checkNotNull(fuseFilePath);
    mBufferSize = bufferSize;
  }

  @Override
  public void run() {
    try (FileInputStream localInputStream = new FileInputStream(mLocalFilePath);
         FileInputStream fuseInputStream = new FileInputStream(mFuseFilePath)) {
      final byte[] localFileBuffer = new byte[mBufferSize];
      final byte[] fuseFileBuffer = new byte[mBufferSize];
      int localBytesRead = 0;
      int fuseBytesRead = 0;
      while (localBytesRead != -1 || fuseBytesRead != -1) {
        localBytesRead = localInputStream.read(localFileBuffer);
        fuseBytesRead = fuseInputStream.read(fuseFileBuffer);
        if (!Utils.isDataCorrect(
            localFileBuffer, fuseFileBuffer, localBytesRead, fuseBytesRead)) {
          System.out.println(String.format(
              Constants.DATA_INCONSISTENCY_FORMAT, Constants.SEQUENTIAL_READ, mBufferSize));
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
