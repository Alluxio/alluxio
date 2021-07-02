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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.io.InputStream;

/**
 * Abstract classes for all integration tests of {@link FileOutStream}.
 */
public abstract class AbstractFileOutStreamIntegrationTest extends BaseIntegrationTest {
  protected static final int MIN_LEN = 0;
  protected static final int MAX_LEN = 255;
  protected static final int DELTA = 32;
  protected static final int BUFFER_BYTES = 100;
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  protected static final int BLOCK_SIZE_BYTES = 1000;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      buildLocalAlluxioClusterResource();

  protected FileSystem mFileSystem = null;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
    if (mLocalAlluxioJobCluster != null) {
      mLocalAlluxioJobCluster.stop();
    }
  }

  /**
   * Override this method in a test in order to customize the {@link LocalAlluxioClusterResource}.
   * @param resource an AlluxioClusterResource builder
   */
  protected void customizeClusterResource(LocalAlluxioClusterResource.Builder resource) {
    resource.setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BUFFER_BYTES)
        .setProperty(PropertyKey.USER_FILE_REPLICATION_DURABLE, 1)
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES);
  }

  private LocalAlluxioClusterResource buildLocalAlluxioClusterResource() {
    LocalAlluxioClusterResource.Builder resource = new LocalAlluxioClusterResource.Builder();
    customizeClusterResource(resource);
    return resource.build();
  }

  /**
   * Helper to write an Alluxio file with stream of bytes of increasing byte value.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   * @param op options to create file
   */
  protected void writeIncreasingBytesToFile(AlluxioURI filePath, int fileLen, CreateFilePOptions op)
      throws Exception {
    writeIncreasingBytesToFile(mFileSystem, filePath, fileLen, op);
  }

  /**
   * Helper to write an Alluxio file with stream of bytes of increasing byte value.
   *
   * @param fs the FileSystemClient to use
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   * @param op options to create file
   */
  protected void writeIncreasingBytesToFile(FileSystem fs, AlluxioURI filePath, int fileLen,
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
  protected void writeIncreasingByteArrayToFile(AlluxioURI filePath, int fileLen,
      CreateFilePOptions op) throws Exception {
    try (FileOutStream os = mFileSystem.createFile(filePath, op)) {
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
  protected void writeTwoIncreasingByteArraysToFile(AlluxioURI filePath, int fileLen,
      CreateFilePOptions op) throws Exception {
    try (FileOutStream os = mFileSystem.createFile(filePath, op)) {
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
  protected void checkFileInAlluxio(AlluxioURI filePath, int fileLen) throws Exception {
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(fileLen, status.getLength());
    try (FileInStream is = mFileSystem.openFile(filePath,
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
  protected void checkFileInUnderStorage(AlluxioURI filePath, int fileLen) throws Exception {
    URIStatus status = mFileSystem.getStatus(filePath);
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
