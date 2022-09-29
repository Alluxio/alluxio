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

package alluxio.client.file;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Add unit tests for {@link UfsBaseFileSystem}.
 */
public class UfsBaseFileSystemTest {
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private AlluxioURI mRootUfs;
  private FileSystem mFileSystem;

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    mRootUfs = new AlluxioURI(ufs);
    mConf.set(PropertyKey.USER_UFS_ENABLED, true, Source.RUNTIME);
    mConf.set(PropertyKey.USER_UFS_ADDRESS, ufs, Source.RUNTIME);
    LocalUnderFileSystemFactory localUnderFileSystemFactory = new LocalUnderFileSystemFactory();
    UnderFileSystemFactoryRegistry.register(localUnderFileSystemFactory);
    mFileSystem = new UfsBaseFileSystem(FileSystemContext.create(
        ClientContext.create(mConf)));
  }

  @After
  public void after() throws IOException {
    FileUtils.deletePathRecursively(mRootUfs.toString());
    mConf = Configuration.copyGlobal();
  }

  // Basic testing
  @Test
  public void createEmptyFileRead() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("emptyFile");
    mFileSystem.createFile(uri).close();
    try (FileInStream inStream = mFileSystem.openFile(uri)) {
      Assert.assertEquals(-1, inStream.read());
    }
  }

  @Test
  public void createFileRead() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("createFileRead");
    int chunkSize = 128;
    try (FileOutStream outStream = mFileSystem.createFile(uri)) {
      outStream.write(BufferUtils.getIncreasingByteArray(chunkSize));
    }
    try (FileInStream inStream = mFileSystem.openFile(uri)) {
      byte[] readRes = new byte[chunkSize];
      Assert.assertEquals(chunkSize, inStream.read(readRes));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(chunkSize, readRes));
    }
  }

  @Test
  public void createDelete() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("createDelete");
    mFileSystem.createFile(uri).close();
    Assert.assertTrue(mFileSystem.exists(uri));
    mFileSystem.delete(uri);
    Assert.assertFalse(mFileSystem.exists(uri));
  }

  @Test
  public void exists() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("exists");
    Assert.assertFalse(mFileSystem.exists(uri));
    mFileSystem.createFile(uri).close();
    Assert.assertTrue(mFileSystem.exists(uri));
  }

  @Test
  public void getFileStatus() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("getFileStatus");
    mFileSystem.createFile(uri).close();
    URIStatus status = mFileSystem.getStatus(uri);
    // TODO(lu) check other field as well
    Assert.assertEquals(uri.toString(), status.getName());
    Assert.assertTrue(status.isCompleted());
    Assert.assertFalse(status.isFolder());
    Assert.assertEquals(0, status.getLength());
    // TODO(lu) create file with actual length
  }

  @Test
  public void getDirectoryStatus() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("getDirectoryStatus");
    mFileSystem.createDirectory(uri);
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(uri.toString(), status.getName());
    Assert.assertTrue(status.isCompleted());
    Assert.assertTrue(status.isFolder());
  }

  @Test
  public void createListDirectory() throws IOException, AlluxioException {
    AlluxioURI dir = mRootUfs.join("createListDirectory");
    mFileSystem.createDirectory(dir);
    Assert.assertEquals(0, mFileSystem.listStatus(dir).size());
    String fileName = "subfile";
    AlluxioURI subFile = dir.join(fileName);
    mFileSystem.createFile(subFile).close();
    List<URIStatus> statuses = mFileSystem.listStatus(dir);
    Assert.assertEquals(1, mFileSystem.listStatus(dir).size());
    Assert.assertEquals(fileName, mFileSystem.listStatus(dir).get(0).getName());
    List<URIStatus> dirStatuses = new ArrayList<>();
    mFileSystem.iterateStatus(dir, dirStatuses::add);
    Assert.assertEquals(1, dirStatuses.size());
    Assert.assertEquals(fileName, dirStatuses.get(0).getName());
  }

  @Test
  public void createDeleteDirectory() throws IOException, AlluxioException {
    AlluxioURI dir = mRootUfs.join("createDeleteDirectory");
    mFileSystem.createDirectory(dir);
    mFileSystem.delete(dir);
    Assert.assertFalse(mFileSystem.exists(dir));
    mFileSystem.createDirectory(dir);
    String fileName = "subfile";
    AlluxioURI subFile = dir.join(fileName);
    mFileSystem.createFile(subFile).close();
    mFileSystem.delete(dir);
    Assert.assertTrue(mFileSystem.exists(dir));
    mFileSystem.delete(dir, DeletePOptions.newBuilder().setRecursive(true).build());
    Assert.assertFalse(mFileSystem.exists(dir));
  }

  @Test
  public void setMode() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("setMode");
    mFileSystem.createFile(uri).close();
    Mode mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ);
    mFileSystem.setAttribute(uri, SetAttributePOptions.newBuilder()
        .setMode(mode.toProto()).build());
    Assert.assertEquals(mode.toShort(), mFileSystem.getStatus(uri).getMode());
  }

  @Test
  public void rename() throws IOException, AlluxioException {
    AlluxioURI src = mRootUfs.join("original");
    AlluxioURI dst = mRootUfs.join("dst");
    mFileSystem.createFile(src).close();
    mFileSystem.rename(src, dst);
    Assert.assertFalse(mFileSystem.exists(src));
    Assert.assertTrue(mFileSystem.exists(dst));
  }
}
