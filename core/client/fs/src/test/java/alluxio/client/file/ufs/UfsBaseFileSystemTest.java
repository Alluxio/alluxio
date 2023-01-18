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

package alluxio.client.file.ufs;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Add unit tests for {@link UfsBaseFileSystem}.
 */
@RunWith(Parameterized.class)
public class UfsBaseFileSystemTest {
  private InstancedConfiguration mConf;
  private AlluxioURI mRootUfs;
  private FileSystem mFileSystem;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {false, false},
        {true, false},
        {false, true},
        {true, true}
    });
  }

  /**
   * Runs {@link UfsBaseFileSystem} with different configuration combinations.
   *
   * @param localDataCached whether local data cache is enabled
   * @param localMetadataCached whether local metadata cache is enabled
   */
  public UfsBaseFileSystemTest(boolean localDataCached, boolean localMetadataCached) {
    mConf = Configuration.copyGlobal();
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED,
        PropertyKey.USER_CLIENT_CACHE_ENABLED.formatValue(localDataCached), Source.RUNTIME);
    mConf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE,
        PropertyKey.USER_METADATA_CACHE_MAX_SIZE.formatValue(localMetadataCached ? 20000 : 0),
        Source.RUNTIME);
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    mRootUfs = new AlluxioURI(ufs);
    UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
    mFileSystem = FileSystem.Factory.create(FileSystemContext.create(
        ClientContext.create(mConf)), FileSystemOptions.create(mConf,
        Optional.of(new UfsFileSystemOptions(ufs))));
  }

  @After
  public void after() throws IOException, AlluxioException {
    mFileSystem.delete(mRootUfs, DeletePOptions.newBuilder().setRecursive(true).build());
    mConf = Configuration.copyGlobal();
  }

  @Test
  public void createEmptyFileRead() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("emptyFile");
    mFileSystem.createFile(uri).close();
    try (FileInStream inStream = mFileSystem.openFile(uri)) {
      Assert.assertEquals(-1, inStream.read());
    }
  }

  @Test
  public void createDelete() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("createDelete");
    Assert.assertFalse(mFileSystem.exists(uri));
    mFileSystem.createFile(uri).close();
    Assert.assertTrue(mFileSystem.exists(uri));
    Assert.assertEquals(0L, mFileSystem.getStatus(uri).getLength());
    mFileSystem.delete(uri);
    Assert.assertFalse(mFileSystem.exists(uri));
  }

  /**
   * Failed in S3. S3 does not have mode concept.
   */
  @Test
  public void createWithMode() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("createWithMode");
    Mode mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ);
    mFileSystem.createFile(uri,
        CreateFilePOptions.newBuilder().setMode(mode.toProto()).build()).close();
    Assert.assertEquals(mode.toShort(), mFileSystem.getStatus(uri).getMode());
  }

  /**
   * Failed in S3. S3 can create file without recursive
   */
  @Test
  public void createWithRecursive() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("nonexistingfolder").join("createWithRecursive");
    Assert.assertThrows(NotFoundRuntimeException.class,
        () -> mFileSystem.createFile(uri).close());
    mFileSystem.createFile(uri, CreateFilePOptions.newBuilder().setRecursive(true).build());
    Assert.assertTrue(mFileSystem.exists(uri));
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
    Assert.assertEquals(1, statuses.size());
    Assert.assertEquals(fileName, statuses.get(0).getName());
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

  /**
   * S3 does not have mode concept.
   */
  @Test
  public void createDirectoryWithMode() throws IOException, AlluxioException {
    AlluxioURI dir = mRootUfs.join("createDirectoryWithMode");
    Mode mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ);
    mFileSystem.createDirectory(dir,
        CreateDirectoryPOptions.newBuilder().setMode(mode.toProto()).build());
    Assert.assertEquals(mode.toShort(), mFileSystem.getStatus(dir).getMode());
  }

  @Test
  public void createDirectoryWithRecursive() throws IOException, AlluxioException {
    AlluxioURI dir = mRootUfs.join("dir1").join("dir2");
    // local filesystem does not need recursive option to create parent
    mFileSystem.createDirectory(dir,
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    Assert.assertTrue(mFileSystem.exists(dir));
  }

  @Test
  public void deleteDirectoryWithRecursive() throws IOException, AlluxioException {
    AlluxioURI parentDir = mRootUfs.join("dir1");
    AlluxioURI currentDir = parentDir.join("dir2");
    mFileSystem.createDirectory(currentDir,
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    // local filesystem does not need recursive option to delete parent + current dir
    mFileSystem.delete(parentDir, DeletePOptions.newBuilder().setRecursive(true).build());
  }

  @Test
  public void getFileStatus() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("getFileStatusFolder").join("getFileStatus");
    mRootUfs.getPath();
    mFileSystem.createFile(uri,
        CreateFilePOptions.newBuilder().setRecursive(true).build()).close();
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(uri.getName(), status.getName());
    Assert.assertEquals(uri.getPath(), status.getPath());
    Assert.assertEquals(uri.toString(), status.getUfsPath());
    Assert.assertTrue(status.isCompleted());
    Assert.assertFalse(status.isFolder());
    Assert.assertEquals(0, status.getLength());
    Assert.assertTrue(status.getOwner() != null && !status.getOwner().isEmpty());
    Assert.assertTrue(status.getGroup() != null && !status.getGroup().isEmpty());
  }

  @Test
  public void getDirectoryStatus() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("getDirectoryStatus");
    mFileSystem.createDirectory(uri);
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(uri.getName(), status.getName());
    Assert.assertEquals(uri.getPath(), status.getPath());
    Assert.assertEquals(uri.toString(), status.getUfsPath());
    Assert.assertTrue(status.isCompleted());
    Assert.assertTrue(status.isFolder());
    Assert.assertTrue(status.getOwner() != null && !status.getOwner().isEmpty());
    Assert.assertTrue(status.getGroup() != null && !status.getGroup().isEmpty());
  }

  /**
   * S3 does not have mode concept.
   */
  @Test
  public void setMode() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("setMode");
    mFileSystem.createFile(uri).close();
    Mode mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ);
    mFileSystem.setAttribute(uri, SetAttributePOptions.newBuilder()
        .setMode(mode.toProto()).build());
    // S3 does not support setting mode/owner/group
    Assert.assertEquals(mode.toShort(), mFileSystem.getStatus(uri).getMode());
  }

  @Test
  public void rename() throws IOException, AlluxioException {
    AlluxioURI src = mRootUfs.join("original");
    AlluxioURI dst = mRootUfs.join("dst");
    mFileSystem.createFile(src).close();
    mFileSystem.rename(src, dst);
    Assert.assertFalse(mFileSystem.exists(src));
    Assert.assertFalse(mFileSystem.getStatus(dst).isFolder());
  }

  @Test
  public void renameDirectory() throws IOException, AlluxioException {
    AlluxioURI src = mRootUfs.join("original");
    AlluxioURI dst = mRootUfs.join("dst");
    mFileSystem.createDirectory(src);
    mFileSystem.rename(src, dst);
    Assert.assertFalse(mFileSystem.exists(src));
    Assert.assertTrue(mFileSystem.getStatus(dst).isFolder());
  }

  @Test
  public void renameWhenDestinationFileExist() throws IOException, AlluxioException {
    AlluxioURI src = mRootUfs.join("original");
    AlluxioURI dst = mRootUfs.join("dst");
    mFileSystem.createFile(src).close();
    mFileSystem.createFile(dst).close();
    // local can overwrite destination file
    // TODO(lu) test S3
    mFileSystem.rename(src, dst);
    Assert.assertFalse(mFileSystem.exists(src));
    Assert.assertFalse(mFileSystem.getStatus(dst).isFolder());
  }

  /**
   * Local can overwrite dest dir but S3 cannot.
   */
  @Test
  public void renameWhenDestinationDirExist() throws IOException, AlluxioException {
    AlluxioURI src = mRootUfs.join("original");
    AlluxioURI dst = mRootUfs.join("dst");
    mFileSystem.createDirectory(src);
    mFileSystem.createDirectory(dst);
    mFileSystem.rename(src, dst);
    Assert.assertFalse(mFileSystem.exists(src));
    Assert.assertTrue(mFileSystem.getStatus(dst).isFolder());
  }

  /**
   * Local can overwrite dest dir but S3 cannot.
   */
  @Test
  public void renameWhenDestinationDirNotEmpty() throws IOException, AlluxioException {
    AlluxioURI src = mRootUfs.join("original");
    AlluxioURI dst = mRootUfs.join("dst");
    AlluxioURI dstFile = dst.join("file");
    mFileSystem.createDirectory(src);
    mFileSystem.createDirectory(dst);
    mFileSystem.createFile(dstFile).close();
    Assert.assertThrows(AlluxioRuntimeException.class, () -> mFileSystem.rename(src, dst));
    Assert.assertTrue(mFileSystem.exists(src));
    Assert.assertTrue(mFileSystem.getStatus(dst).isFolder());
    Assert.assertTrue(mFileSystem.exists(dstFile));
  }

  @Test
  public void writeThenRead() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("writeThenRead");
    int chunkSize = 512;
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
  public void writeThenGetStatus() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("writeThenGetStatus");
    int chunkSize = 512;
    try (FileOutStream outStream = mFileSystem.createFile(uri)) {
      outStream.write(BufferUtils.getIncreasingByteArray(chunkSize));
    }
    Assert.assertEquals(chunkSize, mFileSystem.getStatus(uri).getLength());
  }

/*  @Test
  public void getStatusWhenWriting() throws IOException, AlluxioException {
    AlluxioURI uri = mRootUfs.join("getStatusWhenWriting");
    int chunkSize = 512;
    try (FileOutStream outStream = mFileSystem.createFile(uri)) {
      outStream.write(BufferUtils.getIncreasingByteArray(chunkSize));
      // local & S3 both failed here
      Assert.assertEquals(chunkSize, mFileSystem.getStatus(uri).getLength());
      outStream.write(BufferUtils.getIncreasingByteArray(chunkSize, chunkSize));
      Assert.assertEquals(chunkSize * 2, mFileSystem.getStatus(uri).getLength());
    }
    Assert.assertEquals(chunkSize * 2, mFileSystem.getStatus(uri).getLength());
  }*/
}
