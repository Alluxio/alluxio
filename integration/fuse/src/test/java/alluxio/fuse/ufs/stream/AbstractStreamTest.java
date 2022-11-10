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

package alluxio.fuse.ufs.stream;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.auth.LaunchUserGroupAuthPolicy;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.jnifuse.LibFuse;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * This class includes the shared stream (data) related tests
 * for {@link alluxio.fuse.AlluxioJniFuseFileSystem}
 */
public abstract class AbstractStreamTest {
  protected static final Mode DEFAULT_MODE = new Mode(Mode.Bits.ALL, Mode.Bits.READ, Mode.Bits.READ);
  protected static final int DEFAULT_FILE_LEN = 64;
  protected static final String FILE = "/file";
  protected static final String DIR = "/dir";
  protected static final String EXCEED_LENGTH_PATH_NAME
      = "/path" + String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
  protected AlluxioURI mRootUfs;

  protected FileSystem mFileSystem;
  protected AuthPolicy mAuthPolicy;
  protected FuseFileStream.Factory mStreamFactory;

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf = Configuration.copyGlobal();
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    conf.set(PropertyKey.FUSE_MOUNT_POINT, "/t/mountPoint", Source.RUNTIME);
    conf.set(PropertyKey.USER_UFS_ENABLED, true, Source.RUNTIME);
    conf.set(PropertyKey.USER_ROOT_UFS, ufs, Source.RUNTIME);
    mRootUfs = new AlluxioURI(ufs);
    LocalUnderFileSystemFactory localUnderFileSystemFactory = new LocalUnderFileSystemFactory();
    UnderFileSystemFactoryRegistry.register(localUnderFileSystemFactory);
    FileSystemContext context = FileSystemContext.create(
        ClientContext.create(conf));
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
    mFileSystem = new UfsBaseFileSystem(context);
    mAuthPolicy = LaunchUserGroupAuthPolicy.create(mFileSystem,
        conf, Optional.empty());
    mAuthPolicy.init();
    mStreamFactory = new FuseFileStream.Factory(mFileSystem, mAuthPolicy);
  }

  @After
  public void after() throws IOException {
    FileUtils.deletePathRecursively(mRootUfs.toString());
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
