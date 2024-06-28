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
import alluxio.fuse.correctness.Options;
import alluxio.fuse.correctness.Utils;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * This class is a client verifying the correctness of sequential read of AlluxioFuse.
 */
public class SequentialWriteClient implements Runnable {
  private final Options mOptions;
  private final long mFileSize;

  /**
   * Creates an instance of {@link SequentialWriteClient}.
   *
   * @param options  the options for the validation test
   * @param fileSize the size of the testing file
   */
  public SequentialWriteClient(Options options, long fileSize) {
    mOptions = Preconditions.checkNotNull(options);
    mFileSize = fileSize;
  }

  @Override
  public void run() {
    System.out.println(String.format(
        Constants.TESTING_FILE_SIZE_FORMAT, IOOperation.SequentialWrite, mFileSize));
    String localFilePath = Utils.createFile(mFileSize, mOptions.getLocalDir());
    String fuseFilePath = "";
    for (int bufferSize : Constants.BUFFER_SIZES) {
      try {
        fuseFilePath = writeLocalFileToFuseMountPoint(
            localFilePath, mOptions.getFuseDir(), bufferSize);
        if (!validateData(localFilePath, fuseFilePath)) {
          System.out.println(String.format(
              Constants.DATA_INCONSISTENCY_FORMAT, IOOperation.SequentialWrite, bufferSize));
        }
      } catch (IOException e) {
        System.out.println(String.format(
            "Error writing local file to fuse with file size %d and buffer size %d. %s",
            mFileSize, bufferSize, e));
      }
    }
    Utils.deleteTestFiles(localFilePath, fuseFilePath);
  }

  private static String writeLocalFileToFuseMountPoint(
      String localFilePath, String fuseFileDirPath, int bufferSize) throws IOException {
    File dir = new File(fuseFileDirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    String fuseFilePath = Paths.get(fuseFileDirPath, Long.toString(System.currentTimeMillis()))
        .toString();
    try (FileInputStream localFileInputStream = new FileInputStream(localFilePath);
         FileOutputStream fuseFileOutputStream = new FileOutputStream(fuseFilePath, true)) {
      byte[] buffer = new byte[bufferSize];
      int bytesRead;
      while (true) {
        bytesRead = localFileInputStream.read(buffer);
        if (bytesRead == -1) {
          break;
        }
        fuseFileOutputStream.write(buffer, 0, bytesRead);
      }
    }
    return fuseFilePath;
  }

  private static boolean validateData(String localFilePath, String fuseFilePath) {
    try (FileInputStream localInputStream = new FileInputStream(localFilePath);
         FileInputStream fuseInputStream = new FileInputStream(fuseFilePath)) {
      final byte[] localFileBuffer = new byte[Constants.DEFAULT_BUFFER_SIZE];
      final byte[] fuseFileBuffer = new byte[Constants.DEFAULT_BUFFER_SIZE];
      int localBytesRead = 0;
      int fuseBytesRead = 0;
      while (localBytesRead != -1 || fuseBytesRead != -1) {
        localBytesRead = localInputStream.read(localFileBuffer);
        fuseBytesRead = fuseInputStream.read(fuseFileBuffer);
        if (!Utils.isDataCorrect(
            localFileBuffer, fuseFileBuffer, localBytesRead, fuseBytesRead)) {
          return false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create FileInputStream for validating data. Test is stopped.", e);
    }
    return true;
  }
}
