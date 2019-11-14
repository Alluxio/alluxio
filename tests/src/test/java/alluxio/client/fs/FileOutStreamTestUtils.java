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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;

import java.io.InputStream;

public class FileOutStreamTestUtils {

  /**
   * Helper to write an Alluxio file with stream of bytes of increasing byte value.
   *
   * @param fs the FileSystemClient to use
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   * @param op options to create file
   */
  public static void writeIncreasingBytesToFile(FileSystem fs, AlluxioURI filePath, int fileLen,
      CreateFilePOptions op)
      throws Exception {
    try (FileOutStream os = fs.createFile(filePath, op)) {
      for (int k = 0; k < fileLen; k++) {
        os.write((byte) k);
      }
    }
  }

  /**
   * Helper to write an Alluxio file with one increasing byte array, using a single
   * {@link FileOutStream#write(byte[], int, int)} invocation.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   * @param op options to create file
   */
  public static void writeIncreasingByteArrayToFile(FileSystem fs, AlluxioURI filePath, int fileLen,
      CreateFilePOptions op) throws Exception {
    try (FileOutStream os = fs.createFile(filePath, op)) {
      os.write(BufferUtils.getIncreasingByteArray(fileLen));
    }
  }

  /**
   * Helper to write an Alluxio file with one increasing byte array, but using two separate
   * {@link FileOutStream#write(byte[], int, int)} invocations.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   * @param op options to create file
   */
  public static void writeTwoIncreasingByteArraysToFile(FileSystem fs, AlluxioURI filePath,
      int fileLen,
      CreateFilePOptions op) throws Exception {
    try (FileOutStream os = fs.createFile(filePath, op)) {
      int len1 = fileLen / 2;
      int len2 = fileLen - len1;
      os.write(BufferUtils.getIncreasingByteArray(0, len1), 0, len1);
      os.write(BufferUtils.getIncreasingByteArray(len1, len2), 0, len2);
    }
  }

  /**
   * Checks the given file exists in Alluxio storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   */
  public static void checkFileInAlluxio(FileSystem fs, AlluxioURI filePath, int fileLen)
      throws Exception {
    URIStatus status = fs.getStatus(filePath);
    Assert.assertEquals(fileLen, status.getLength());
    try (FileInStream is = fs.openFile(filePath,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
      byte[] res = new byte[(int) status.getLength()];
      Assert.assertEquals((int) status.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileLen, res));
    }
  }

  /**
   * Checks the given file exists in under storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   */
  public static void checkFileInUnderStorage(FileSystem fs, AlluxioURI filePath, int fileLen)
      throws Exception {
    URIStatus status = fs.getStatus(filePath);
    String checkpointPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(checkpointPath,
        ServerConfiguration.global());

    try (InputStream is = ufs.open(checkpointPath)) {
      byte[] res = new byte[(int) status.getLength()];
      int totalBytesRead = 0;
      while (true) {
        int bytesRead = is.read(res, totalBytesRead, res.length - totalBytesRead);
        if (bytesRead <= 0) {
          break;
        }
        totalBytesRead += bytesRead;
      }
      Assert.assertEquals((int) status.getLength(), totalBytesRead);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileLen, res));
    }
  }
}
