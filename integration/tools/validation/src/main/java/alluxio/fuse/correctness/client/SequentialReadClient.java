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
import alluxio.fuse.correctness.IOOperation;
import alluxio.fuse.correctness.Utils;

import com.google.common.base.Preconditions;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * This class is a client verifying the correctness of sequential read of AlluxioFuse.
 */
public class SequentialReadClient implements Runnable {
  private final List<String> mLocalFileList;
  private final List<String> mFuseFileList;
  private final int mBufferSize;
  private final boolean mLongRunning;

  /**
   * Creates an instance of {@link SequentialReadClient}.
   *
   * @param localFileList list of test files in the local file system
   * @param fuseFileList  list of test files in the fuse mount point
   * @param bufferSize    size of the buffer of the client
   * @param longRunning   whether the client is a long-running client
   */
  public SequentialReadClient(List<String> localFileList, List<String> fuseFileList,
      int bufferSize, boolean longRunning) {
    mLocalFileList = Preconditions.checkNotNull(localFileList);
    mFuseFileList = Preconditions.checkNotNull(fuseFileList);
    mBufferSize = bufferSize;
    mLongRunning = longRunning;
  }

  @Override
  public void run() {
    do {
      int index = Utils.RANDOM.nextInt(mLocalFileList.size());
      try (FileInputStream localInputStream = new FileInputStream(mLocalFileList.get(index));
           FileInputStream fuseInputStream = new FileInputStream(mFuseFileList.get(index))) {
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
                Constants.DATA_INCONSISTENCY_FORMAT, IOOperation.SequentialRead, mBufferSize));
          }
          if (Thread.interrupted()) {
            return;
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Some thread caught IOException. Test is stopped.", e);
      }
    } while (mLongRunning);
  }
}
