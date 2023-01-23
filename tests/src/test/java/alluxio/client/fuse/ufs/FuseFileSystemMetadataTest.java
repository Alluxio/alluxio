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

package alluxio.client.fuse.ufs;

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.fuse.AlluxioJniRenameUtils;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.security.authorization.Mode;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Isolation tests for {@link alluxio.fuse.AlluxioJniFuseFileSystem} with local UFS.
 * This test covers the basic file system metadata operations.
 */
public class FuseFileSystemMetadataTest extends AbstractFuseFileSystemTest {

  @Test
  public void createDeleteFile() {
    createEmptyFile(FILE);
    Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(0, mFuseFs.unlink(FILE));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, mFileStat));
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

  @Test
  public void overwriteExistingDirectoryLocalS3Ufs() {
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
    Assert.assertEquals(0, mFuseFs.mkdir(DIR, DEFAULT_MODE.toShort()));
  }

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
}
