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

package alluxio.client.fuse.dora.readonly;

import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_RDWR;

import alluxio.jnifuse.ErrorCodes;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests for testing Fuse with dora metadata/data cache in read-only workloads.
 */
public class FuseDoraReadOnlyTest extends AbstractFuseFileSystemTest {
  @Test
  public void readWriteEmptyNonExisting() {
    mFileInfo.get().flags.set(O_RDWR.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, mFileStat));
  }

  @Test
  public void emptyFileRead() throws IOException {
    createEmptyFile(UFS_ROOT.join(FILE).toString());
    Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(0, mFileStat.st_size.intValue());
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
    Assert.assertEquals(0,
        mFuseFs.read(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void readExisting() throws IOException {
    createFile(UFS_ROOT.join(FILE).toString(), DEFAULT_FILE_LEN);
    // read from file
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_FILE_LEN);
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.read(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, buffer);
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }
}
