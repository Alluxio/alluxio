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

package alluxio.client.fuse.ufs.stream;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.fuse.ufs.AbstractFuseDoraTest;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.auth.LaunchUserGroupAuthPolicy;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.security.authorization.Mode;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;

import java.util.Optional;
import java.util.UUID;

/**
 * This class includes the shared stream related tests
 * for {@link FuseFileStream} with local UFS.
 */
public abstract class AbstractStreamTest extends AbstractFuseDoraTest {
  protected static final Mode DEFAULT_MODE = new Mode(
      Mode.Bits.ALL, Mode.Bits.READ, Mode.Bits.READ);
  protected FuseFileStream.Factory mStreamFactory;

  @Before
  public void beforeActions() {
    AuthPolicy authPolicy = LaunchUserGroupAuthPolicy.create(mFileSystem,
        mContext.getClusterConf(), Optional.empty());
    authPolicy.init();
    mStreamFactory = new FuseFileStream.Factory(mFileSystem, authPolicy);
  }

  @Override
  public void afterActions() {}

  protected AlluxioURI getTestFileUri() {
    return new AlluxioURI("/file" + UUID.randomUUID());
  }

  /**
   * Helper to write an Alluxio file with one increasing byte array, using a single
   * {@link FileOutStream#write(byte[], int, int)} invocation.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   */
  protected void writeIncreasingByteArrayToFile(AlluxioURI filePath, int fileLen) throws Exception {
    try (FileOutStream os = mFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setRecursive(true).build())) {
      os.write(BufferUtils.getIncreasingByteArray(fileLen));
    }
  }

  /**
   * Checks the given file exists in Alluxio storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   */
  protected void checkFile(AlluxioURI filePath, int fileLen, int startValue)
      throws Exception {
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(fileLen, status.getLength());
    try (FileInStream is = mFileSystem.openFile(filePath,
        OpenFilePOptions.newBuilder().build())) {
      byte[] res = new byte[(int) status.getLength()];
      Assert.assertEquals((int) status.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(startValue, fileLen, res));
    }
  }
}
