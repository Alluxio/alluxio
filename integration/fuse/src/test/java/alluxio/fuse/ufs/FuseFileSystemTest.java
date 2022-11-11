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

import alluxio.Constants;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.fuse.options.FuseOptions;
import alluxio.fuse.ufs.AbstractTest;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.security.authorization.Mode;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Isolation tests for {@link AlluxioJniFuseFileSystem} with local UFS.
 * This test covers the basic file system operations.
 */
public class FuseFileSystemTest extends AbstractTest {
  protected AlluxioJniFuseFileSystem mFuseFs;
  protected AlluxioFuseUtils.CloseableFuseFileInfo mFileInfo;
  protected FileStat mFileStat;

  @Override
  public void beforeActions() {
    mFuseFs = new AlluxioJniFuseFileSystem(mContext, mFileSystem,
        new FuseOptions(FileSystemOptions.create(
            mContext.getClusterConf(), Optional.of(mUfsOptions))));
    mFileStat = FileStat.of(ByteBuffer.allocateDirect(256));
    mFileInfo = new AlluxioFuseUtils.CloseableFuseFileInfo();
  }

  @Override
  public void afterActions() throws IOException {
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
    Mode res = new Mode(mFileStat.st_mode.shortValue());
    Assert.assertEquals(DEFAULT_MODE.getOwnerBits(), res.getOwnerBits());
    Assert.assertEquals(DEFAULT_MODE.getGroupBits(), res.getGroupBits());
    Assert.assertEquals(DEFAULT_MODE.getOtherBits(), res.getOtherBits());
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
  public void createCloseDifferentThread() throws InterruptedException, ExecutionException {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, DEFAULT_MODE.toShort(), mFileInfo.get()));
    Callable<Integer> releaseTask = () -> mFuseFs.release(FILE, mFileInfo.get());
    ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
    Future<Integer> releaseResult = threadExecutor.submit(releaseTask);
    Assert.assertEquals(0, (int) releaseResult.get());

    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    releaseResult = threadExecutor.submit(releaseTask);
    Assert.assertEquals(0, (int) releaseResult.get());
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
  public void openTimeoutWhenIncomplete() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE, DEFAULT_MODE.toShort(), mFileInfo.get()));
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(-ErrorCodes.ETIME(), mFuseFs.open(FILE, mFileInfo.get()));
  }

  @Test
  public void getAttrNonExisting() {
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, mFileStat));
  }

  @Test
  public void createDirectory() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.getattr(DIR, mFileStat));
    // s3 will always be 700
    Mode res = new Mode(mFileStat.st_mode.shortValue());
    Assert.assertEquals(DEFAULT_MODE.getOwnerBits(), res.getOwnerBits());
    Assert.assertEquals(DEFAULT_MODE.getGroupBits(), res.getGroupBits());
    Assert.assertEquals(DEFAULT_MODE.getOtherBits(), res.getOtherBits());
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
  public void rmdir() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.getattr(DIR, mFileStat));
    Assert.assertEquals(0, mFuseFs.rmdir(DIR));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(DIR, mFileStat));
  }

  @Test
  public void rmdirNotEmpty() {
    Assert.assertEquals(0, mFuseFs.mkdir("dir", DEFAULT_MODE.toShort()));
    createEmptyFile("/dir/file");
    Assert.assertEquals(0, mFuseFs.rmdir(DIR));
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

  @Test
  public void getAttrWhenWriting() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    int len = 20;
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(20);
    Assert.assertEquals(len,
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(len, mFileStat.st_size.intValue());
    // s3 will always be 700
    Mode res = new Mode(mFileStat.st_mode.shortValue());
    Assert.assertEquals(DEFAULT_MODE.getOwnerBits(), res.getOwnerBits());
    Assert.assertEquals(DEFAULT_MODE.getGroupBits(), res.getGroupBits());
    Assert.assertEquals(DEFAULT_MODE.getOtherBits(), res.getOtherBits());
    Assert.assertEquals(AlluxioFuseUtils.getSystemUid(), mFileStat.st_uid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), mFileStat.st_gid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), mFileStat.st_gid.get());
    Assert.assertTrue((mFileStat.st_mode.intValue() & FileStat.S_IFREG) != 0);
    buffer.flip();
    Assert.assertEquals(len,
        mFuseFs.write(FILE, buffer, len, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(len * 2, mFileStat.st_size.intValue());
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(len * 2, mFileStat.st_size.intValue());
  }
}
