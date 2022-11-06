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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.fuse.AlluxioFuseUtils.CloseableFuseFileInfo;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.LibFuse;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.s3a.S3AUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

/**
 * Isolation tests for {@link AlluxioJniFuseFileSystem} with S3.
 */
public class JniFuseS3FileSystemTest {
  private static final Mode DEFAULT_MODE = new Mode(Mode.Bits.ALL, Mode.Bits.READ, Mode.Bits.READ);
  private static final String MOUNT_POINT = "/t/mountPoint";
  private static final String FILE = "/file";
  private static final String DIR = "/dir";
  private static final String EXCEED_LENGTH_PATH_NAME
      = "/path" + String.join("", Collections.nCopies(16, "0123456789ABCDEF"));
  private AlluxioURI mRootUfs;
  private AlluxioJniFuseFileSystem mFuseFs;
  private CloseableFuseFileInfo mFileInfo;
  private FileStat mFileStat;

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf = Configuration.copyGlobal();
    String ufs = "s3://alluxio-test-fuse/lu" + UUID.randomUUID();
    conf.set(PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT, Source.RUNTIME);
    conf.set(PropertyKey.USER_UFS_ENABLED, true, Source.RUNTIME);
    conf.set(PropertyKey.USER_ROOT_UFS, ufs, Source.RUNTIME);
    mRootUfs = new AlluxioURI(ufs);
    S3AUnderFileSystemFactory s3aUnderFileSystemFactory = new S3AUnderFileSystemFactory();
    UnderFileSystemFactoryRegistry.register(s3aUnderFileSystemFactory);
    FileSystemContext context = FileSystemContext.create(
        ClientContext.create(conf));
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
    mFuseFs = new AlluxioJniFuseFileSystem(context, new UfsBaseFileSystem(context));
    mFileInfo = new CloseableFuseFileInfo();
    mFileStat = FileStat.of(ByteBuffer.allocateDirect(256));
  }

  @After
  public void after() throws IOException {
    FileUtils.deletePathRecursively(mRootUfs.toString());
    BufferUtils.cleanDirectBuffer(mFileStat.getBuffer());
    mFileInfo.close();
  }

  @Test
  public void createEmpty() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, DEFAULT_MODE.toShort(), mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(0, mFileStat.st_size.longValue());
    // s3 will always be 700
  }

  @Test
  public void createOverwriteEmpty() {
    createEmptyFile(FILE);
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, DEFAULT_MODE.toShort(), mFileInfo.get()));
    int len = 4;
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(len);
    Assert.assertEquals(len,
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(4, mFileStat.st_size.intValue());
  }

  @Test
  public void createExisting() {
    int len = 4;
    createFile(FILE, len);
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, DEFAULT_MODE.toShort(), mFileInfo.get()));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(len);
    Assert.assertEquals(-ErrorCodes.EEXIST(),
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void createDeleteFile() {
    createEmptyFile(FILE);
    Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(0, mFuseFs.unlink(FILE));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, mFileStat));
  }

  @Test
  public void createWithLengthLimit() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.create(EXCEED_LENGTH_PATH_NAME, DEFAULT_MODE.toShort(), mFileInfo.get()));
  }

  @Test
  public void openEmpty() {
    createEmptyFile(FILE);
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void openWriteNonExisting() {
    createEmptyFile(FILE);
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void openOverwriteEmpty() {
    createEmptyFile(FILE);
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
  }

  @Test
  public void openWriteExisting() {
    int len = 4;
    createFile(FILE, len);
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(len);
    Assert.assertEquals(-ErrorCodes.EEXIST(),
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void openReadNonExisting() {
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.open(FILE, mFileInfo.get()));
  }

  @Test
  public void openWithLengthLimit() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.create(EXCEED_LENGTH_PATH_NAME, DEFAULT_MODE.toShort(), mFileInfo.get()));
  }

  @Test
  public void getAttrNonExisting() {
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, mFileStat));
  }

  @Test
  public void createDirectory() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.getattr(DIR, mFileStat));
  }

  /**
   * Local UFS supports overwrite existing directory. TODO(lu) S3
   */
  @Test
  public void createExistingDirectory() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
  }

  @Test
  public void createDirectoryWithLengthLimit() {
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.mkdir(EXCEED_LENGTH_PATH_NAME, DEFAULT_MODE.toShort()));
  }

  @Test
  public void createDeleteDirectory() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.getattr(DIR, mFileStat));
    Assert.assertEquals(0, mFuseFs.unlink(DIR));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(DIR, mFileStat));
  }

  @Test
  public void createDeleteNonEmptyDirectory() {
    Assert.assertEquals(0, mFuseFs.mkdir("dir", DEFAULT_MODE.toShort()));
    createEmptyFile("/dir/file");
    Assert.assertEquals(0, mFuseFs.unlink(DIR));
  }

  @Test
  public void renameFile() {
    String src = "/src";
    String dst = "/dst";
    createEmptyFile(src);
    Assert.assertEquals(0, mFuseFs.getattr(src, mFileStat));
    Assert.assertEquals(0, mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(src, mFileStat));
    Assert.assertEquals(0, mFuseFs.getattr(dst, mFileStat));
  }

  @Test
  public void renameDirectory() {
    String src = "/src";
    String dst = "/dst";
    Assert.assertEquals(0, mFuseFs.mkdir(src, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.getattr(src, mFileStat));
    mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS);
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(src, mFileStat));
    Assert.assertEquals(0, mFuseFs.getattr(dst, mFileStat));
  }

  @Test
  public void renameSrcNotExist() {
    assertEquals(-ErrorCodes.ENOENT(),
        mFuseFs.rename("/src", "/dst", AlluxioJniRenameUtils.NO_FLAGS));
  }

  // TODO(lu) different rename flags
  @Test
  public void renameDstFileExist() {
    String src = "/src";
    String dst = "/dst";
    createEmptyFile(src);
    createEmptyFile(dst);
    Assert.assertEquals(0, mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void renameDstDirectoryExist() {
    String src = "/src";
    String dst = "/dst";
    Assert.assertEquals(0, mFuseFs.mkdir(src, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.mkdir(dst, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void renameWithLengthLimit() {
    String src = "/src";
    createEmptyFile(src);
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.rename(src, EXCEED_LENGTH_PATH_NAME, AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void statfs() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4 * Constants.KB);
    try {
      buffer.clear();
      Statvfs stbuf = Statvfs.of(buffer);
      assertEquals(0, mFuseFs.statfs("/", stbuf));
    } finally {
      BufferUtils.cleanDirectBuffer(buffer);
    }
  }

  /**
   * Local UFS supports, S3 does not support.
   */
  @Test
  public void chmod() {
    createEmptyFile(FILE);
    Mode mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ);
    mFuseFs.chmod(FILE, mode.toShort());
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    Assert.assertEquals(0, mFuseFs.getattr(FILE, stat));
    Mode res = new Mode(stat.st_mode.shortValue());
    Assert.assertEquals(mode.getOwnerBits(), res.getOwnerBits());
    Assert.assertEquals(mode.getGroupBits(), res.getGroupBits());
    Assert.assertEquals(mode.getOtherBits(), res.getOtherBits());
  }

  @Test
  public void truncate() {
    // follow existing test cases
  }

  @Test
  public void createDeleteFileCircular() {
    // TODO(lu) iteration to keep create/delete files and make sure it all succeed
  }

  @Test
  public void createDeleteFileGetStatusCircular() {
    // TODO(lu) iteration to keep create/delete files and make sure it all succeed
  }

  @Test
  public void createDeleteDirectoryCircular() {
    // TODO(lu) iteration to keep create/delete files and make sure it all succeed
  }

  @Test
  public void writeThenRead() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    int len = 20;
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(len);
    Assert.assertEquals(len,
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    // read from file
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    buffer.flip();
    Assert.assertEquals(len,
        mFuseFs.read(FILE, buffer, len, 0, mFileInfo.get()));
    BufferUtils.equalIncreasingByteBuffer(0, len, buffer);
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void getAttrWhenWriting() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    int len = 20;
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(20);
    // Expected 20, now -17
    Assert.assertEquals(len,
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(len, mFileStat.st_size.intValue());
    buffer.flip();
    Assert.assertEquals(len,
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(len * 2, mFileStat.st_size.intValue());
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(len * 2, mFileStat.st_size.intValue());
  }

  @Test
  public void readingWhenWriting() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    try {
      int len = 20;
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(20);
      Assert.assertEquals(len,
          mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));

      mFileInfo.get().flags.set(O_RDONLY.intValue());
      Assert.assertEquals(-ErrorCodes.ETIME(), mFuseFs.open(FILE, mFileInfo.get()));
    } finally {
      mFuseFs.release(FILE, mFileInfo.get());
    }
  }

  private void createEmptyFile(String path) {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path, DEFAULT_MODE.toShort(), mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
  }

  private void createFile(String path, int size) {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path, DEFAULT_MODE.toShort(), mFileInfo.get()));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(size);
    Assert.assertEquals(size,
        mFuseFs.write(FILE, buffer, size, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
  }
  // TODO(lu) separate streams
  // TODO(lu) hard to test: readdir, chown
}
