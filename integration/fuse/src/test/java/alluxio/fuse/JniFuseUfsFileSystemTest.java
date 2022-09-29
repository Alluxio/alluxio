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

package alluxio.fuse;

import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_WRONLY;
import static org.junit.Assert.assertEquals;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Isolation tests for {@link AlluxioJniFuseFileSystem} with local UFS.
 */
public class JniFuseUfsFileSystemTest {
  private static final String MOUNT_POINT = "/t/mountPoint";
  private static final String FILE = "/file";
  private static final String DIR = "/dir";

  private AlluxioURI mRootUfs;
  private AlluxioJniFuseFileSystem mFuseFs;
  private FuseFileInfo mFileInfo;

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf = Configuration.copyGlobal();
    String ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    conf.set(PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT, Source.RUNTIME);
    conf.set(PropertyKey.USER_UFS_ENABLED, true, Source.RUNTIME);
    conf.set(PropertyKey.USER_UFS_ADDRESS, ufs, Source.RUNTIME);
    mRootUfs = new AlluxioURI(ufs);
    LocalUnderFileSystemFactory localUnderFileSystemFactory = new LocalUnderFileSystemFactory();
    UnderFileSystemFactoryRegistry.register(localUnderFileSystemFactory);
    FileSystemContext context = FileSystemContext.create(
        ClientContext.create(conf));
    mFuseFs = new AlluxioJniFuseFileSystem(context, new UfsBaseFileSystem(context));
    mFileInfo = allocateNativeFileInfo();
  }

  @After
  public void after() throws IOException {
    FileUtils.deletePathRecursively(mRootUfs.toString());
  }

  @Test
  public void createEmpty() {
    mFileInfo.flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, 0, mFileInfo));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo));
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    Assert.assertEquals(0, mFuseFs.getattr(FILE, stat));
    Assert.assertEquals(0, stat.st_size.longValue());
  }

  @Test
  public void createDeleteFile() {
    createEmptyFile(FILE);
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    Assert.assertEquals(0, mFuseFs.getattr(FILE, stat));
    Assert.assertEquals(0, mFuseFs.unlink(FILE));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, stat));
  }

  @Test
  public void createDirectory() {
    long mode = 493L;
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, mode));
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    Assert.assertEquals(0, mFuseFs.getattr(DIR, stat));
    mode |= FileStat.S_IFDIR;
    Assert.assertEquals(mode, stat.st_mode.intValue());
  }

  @Test
  public void createDeleteDirectory() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, 493L));
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    Assert.assertEquals(0, mFuseFs.getattr(DIR, stat));
    Assert.assertEquals(0, mFuseFs.unlink(DIR));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(DIR, stat));
  }

  @Test
  public void createWithLengthLimit() {
    String c256 = String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
    mFileInfo.flags.set(O_WRONLY.intValue());
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.create(FILE + c256, 0, mFileInfo));
  }

  @Test
  public void mkDirWithLengthLimit() {
    String c256 = String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.mkdir(DIR + c256, 493L));
  }

  @Test
  public void openEmpty() {
    createEmptyFile(FILE);
    mFileInfo.flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo));
  }

  @Test
  public void writeThenRead() {
    mFileInfo.flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, 0, mFileInfo));
    // write to file
    byte[] expected = {42, -128, 1, 3};
    ByteBuffer buffer = ByteBuffer.allocateDirect(expected.length);
    buffer.put(expected, 0, expected.length);
    buffer.flip();
    Assert.assertEquals(expected.length,
        mFuseFs.write(FILE, buffer, expected.length, 0, mFileInfo));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo));
    // read from file
    mFileInfo.flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo));
    ByteBuffer readBuffer = ByteBuffer.allocateDirect(expected.length);
    Assert.assertEquals(expected.length,
        mFuseFs.read(FILE, readBuffer, expected.length, 0, mFileInfo));
    readBuffer.flip();
    final byte[] dst = new byte[expected.length];
    readBuffer.get(dst, 0, expected.length);
    Assert.assertArrayEquals(expected, dst);
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo));
  }

  @Test
  public void rename() {
    String src = "/src";
    String dst = "/dst";
    createEmptyFile(src);
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    Assert.assertEquals(0, mFuseFs.getattr(src, stat));
    mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS);
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(src, stat));
    Assert.assertEquals(0, mFuseFs.getattr(dst, stat));
  }

  @Test
  public void renameSrcNotExist() {
    assertEquals(-ErrorCodes.ENOENT(),
        mFuseFs.rename("/src", "/dst", AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void renameWithLengthLimit() {
    String c256 = String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
    String src = "/src";
    String dst = "/dst" + c256;
    createEmptyFile(src);
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void statfs() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4 * Constants.KB);
    buffer.clear();
    Statvfs stbuf = Statvfs.of(buffer);
    assertEquals(0, mFuseFs.statfs("/", stbuf));
  }

  private void createEmptyFile(String path) {
    mFileInfo.flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path, 0, mFileInfo));
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo));
  }

  // Allocate native memory for a FuseFileInfo data struct and return its pointer
  private FuseFileInfo allocateNativeFileInfo() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(36);
    buffer.clear();
    return FuseFileInfo.of(buffer);
  }
  // TODO(lu) readdir, incomplete file read when write testing
}
