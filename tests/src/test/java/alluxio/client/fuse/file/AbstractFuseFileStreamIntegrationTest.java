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

package alluxio.client.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.auth.LaunchUserGroupAuthPolicy;
import alluxio.fuse.file.FuseFileStream;
import alluxio.fuse.metadata.FuseMetadataSystem;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.util.Optional;

/**
 * Abstract classes for all integration tests of {@link FuseFileStream}.
 */
public abstract class AbstractFuseFileStreamIntegrationTest extends BaseIntegrationTest {
  protected static final int DEFAULT_FILE_LEN = 64;
  protected static final long MODE = 10644;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.FUSE_AUTH_POLICY_CLASS,
              "alluxio.fuse.auth.LaunchUserGroupAuthPolicy")
          .build();

  protected FileSystem mFileSystem = null;
  protected AuthPolicy mAuthPolicy = null;
  protected FuseMetadataSystem mMetadataCache = null;
  protected FuseFileStream.Factory mStreamFactory = null;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    AlluxioConfiguration conf = mFileSystem.getConf();
    mAuthPolicy = LaunchUserGroupAuthPolicy.create(mFileSystem,
        AlluxioFuseFileSystemOpts.create(conf), Optional.empty());
    mAuthPolicy.init();
    mMetadataCache = new FuseMetadataSystem(mFileSystem, conf);
    mStreamFactory = new FuseFileStream.Factory(mFileSystem, mAuthPolicy, mMetadataCache);
  }

  /**
   * Helper to write an Alluxio file with one increasing byte array, using a single
   * {@link FileOutStream#write(byte[], int, int)} invocation.
   *
   * @param filePath path of the tmp file
   * @param fileLen length of the file
   */
  protected void writeIncreasingByteArrayToFile(AlluxioURI filePath, int fileLen) throws Exception {
    try (FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.MUST_CACHE).setRecursive(true).build())) {
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
  protected void checkFileInAlluxio(AlluxioURI filePath, int fileLen, int startValue)
      throws Exception {
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(fileLen, status.getLength());
    try (FileInStream is = mFileSystem.openFile(filePath,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
      byte[] res = new byte[(int) status.getLength()];
      Assert.assertEquals((int) status.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(startValue, fileLen, res));
    }
  }
}
