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

package alluxio.dora.client.fuse.dora;

import static org.junit.Assert.assertEquals;

import alluxio.dora.Constants;
import alluxio.fuse.AlluxioJniRenameUtils;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.dora.security.authorization.Mode;
import alluxio.dora.util.io.BufferUtils;

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
    String path = "/createDeleteFile";
    createEmptyFile(path);
    assertEquals(0, mFuseFs.getattr(path, mFileStat));
    assertEquals(0, mFuseFs.unlink(path));
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(path, mFileStat));
  }

  @Test
  public void getAttrNonExisting() {
    String path = "/getAttrNonExisting";
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(path, mFileStat));
  }

  @Test
  public void createDirectory() {
    String path = "/createDirectory";
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.getattr(path, mFileStat));
  }

  @Test
  public void createDirectoryWithLengthLimit() {
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.mkdir(EXCEED_LENGTH_PATH_NAME, DEFAULT_MODE.toShort()));
  }

  @Test
  public void createDeletepathectory() {
    String path = "/createDeletepathectory";
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.getattr(path, mFileStat));
    assertEquals(0, mFuseFs.unlink(path));
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(path, mFileStat));
  }

  @Test
  public void createDeleteNonEmptyDirectory() {
    String path = "/createDeleteNonEmptyDirectory";
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
    createEmptyFile(path + "/file");
    assertEquals(0, mFuseFs.unlink(path));
  }

  @Test
  public void rmdir() {
    String path = "/rmdir";
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.getattr(path, mFileStat));
    assertEquals(0, mFuseFs.rmdir(path));
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(path, mFileStat));
  }

  @Test
  public void rmdirNotEmpty() {
    String path = "/rmdirNotEmpty";
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
    createEmptyFile(path + "/file");
    assertEquals(0, mFuseFs.rmdir(path));
  }

  @Test
  public void renameFile() {
    String src = "/src";
    String dst = "/dst";
    createEmptyFile(src);
    assertEquals(0, mFuseFs.getattr(src, mFileStat));
    assertEquals(0, mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(src, mFileStat));
    assertEquals(0, mFuseFs.getattr(dst, mFileStat));
  }

  @Test
  public void renameDirectory() {
    String src = "/src";
    String dst = "/dst";
    assertEquals(0, mFuseFs.mkdir(src, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.getattr(src, mFileStat));
    mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS);
    assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(src, mFileStat));
    assertEquals(0, mFuseFs.getattr(dst, mFileStat));
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
    assertEquals(0, mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
  }

  @Test
  public void renameDstDirectoryExist() {
    String src = "/src";
    String dst = "/dst";
    assertEquals(0, mFuseFs.mkdir(src, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.mkdir(dst, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.rename(src, dst, AlluxioJniRenameUtils.NO_FLAGS));
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
    String path = "/overwriteExistingDirectoryLocalS3Ufs";
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
    assertEquals(0, mFuseFs.mkdir(path, DEFAULT_MODE.toShort()));
  }

  @Test
  public void chmod() {
    String path = "/chmod";
    createEmptyFile(path);
    Mode mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ);
    mFuseFs.chmod(path, mode.toShort());
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    assertEquals(0, mFuseFs.getattr(path, stat));
    Mode res = new Mode(stat.st_mode.shortValue());
    Assert.assertEquals(mode.getOwnerBits(), res.getOwnerBits());
    Assert.assertEquals(mode.getGroupBits(), res.getGroupBits());
    Assert.assertEquals(mode.getOtherBits(), res.getOtherBits());
  }
}
