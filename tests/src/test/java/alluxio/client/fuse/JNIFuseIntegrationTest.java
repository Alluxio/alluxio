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

package alluxio.client.fuse;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.AlluxioFuseUtils.CloseableFuseFileInfo;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.options.FuseOptions;
import alluxio.jnifuse.LibFuse;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.util.io.BufferUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Integration tests for JNR-FUSE based {@link AlluxioJniFuseFileSystem}.
 */
public class JNIFuseIntegrationTest extends AbstractFuseIntegrationTest {
  private static final int FILE_LEN = 128;
  private AlluxioJniFuseFileSystem mFuseFileSystem;

  @Override
  public void configure() {
    Configuration.set(PropertyKey.FUSE_JNIFUSE_ENABLED, true);
  }

  @Override
  public void mountFuse(FileSystemContext context,
      FileSystem fileSystem, String mountPoint, String alluxioRoot) {
    Configuration.set(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH, alluxioRoot);
    Configuration.set(PropertyKey.FUSE_MOUNT_POINT, mountPoint);
    AlluxioConfiguration conf = Configuration.global();
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(conf));
    mFuseFileSystem = new AlluxioJniFuseFileSystem(context, fileSystem, FuseOptions.create(conf));
    mFuseFileSystem.mount(false, false, new String[] {});
  }

  @Override
  public void beforeStop() throws IOException {
    try {
      mFuseFileSystem.umount(true);
    } catch (Exception e) {
      // will try umounting from shell
    }
    umountFromShellIfMounted();
  }

  @Override
  public void afterStop() {
    // noop
  }

  /**
   * Tests creating a file for writing
   * and opening a file for O_RDONLY read-only open flag.
   */
  @Test
  public void createWriteOpenRead() throws Exception {
    String testFile = "/createWriteOpenReadTestFile";
    try (CloseableFuseFileInfo info = new CloseableFuseFileInfo()) {
      FuseFileInfo fuseFileInfo = info.get();

      // cannot open non-existing file for read
      fuseFileInfo.flags.set(OpenFlags.O_RDONLY.intValue());
      Assert.assertNotEquals(0, mFuseFileSystem.open(testFile, fuseFileInfo));

      // open existing file for read
      createTestFile(testFile, fuseFileInfo, FILE_LEN);
      readAndValidateTestFile(testFile, fuseFileInfo, FILE_LEN);
    }
  }

  /**
   * Tests opening a file with O_WRONLY flag on non-existing file.
   */
  @Test
  public void openWriteNonExisting() throws Exception {
    String testFile = "/openWriteNonExisting";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      info.flags.set(OpenFlags.O_WRONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
      try {
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info));
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      }
      readAndValidateTestFile(testFile, info, FILE_LEN);
    }
  }

  /**
   * Tests opening a file with O_WRONLY on existing file without O_TRUNC.
   */
  @Test
  public void openWriteExistingWithoutTruncFlag() throws Exception {
    String testFile = "/openWriteExistingWithoutTruncFlag";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      createTestFile(testFile, info, FILE_LEN);

      info.flags.set(OpenFlags.O_WRONLY.intValue());
      try {
        Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        // O_WRONLY without O_TRUNC cannot overwrite
        Assert.assertTrue(mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info) < 0);
      } finally {
        mFuseFileSystem.release(testFile, info);
      }
    }
  }

  /**
   * Tests opening a file with O_WRONLY and O_TRUNC flags to overwrite existing file.
   */
  @Test
  public void openWriteExistingWithTruncFlag() throws Exception {
    String testFile = "/openWriteExistingWithTruncFlag";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      createTestFile(testFile, info, FILE_LEN / 2);

      info.flags.set(OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue());
      try {
        Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info));
      } finally {
        mFuseFileSystem.release(testFile, info);
      }
      readAndValidateTestFile(testFile, info, FILE_LEN);
    }
  }

  /**
   * Tests opening a file with O_WRONLY and then truncating it to zero
   * length for writing.
   */
  @Test
  public void openWriteExistingWithTruncate() throws Exception {
    String testFile = "/openWriteExistingWithTruncate";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      createTestFile(testFile, info, FILE_LEN / 2);

      info.flags.set(OpenFlags.O_WRONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      try {
        // truncate to original length is no-op
        Assert.assertEquals(0, mFuseFileSystem.truncate(testFile, FILE_LEN / 2));
        // truncate to a large value
        Assert.assertNotEquals(0, mFuseFileSystem.truncate(testFile, FILE_LEN));
        // delete file
        Assert.assertEquals(0, mFuseFileSystem.truncate(testFile, 0));
        Assert.assertEquals(0, mFuseFileSystem.truncate(testFile, FILE_LEN * 2));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info));
      } finally {
        mFuseFileSystem.release(testFile, info);
      }
      info.flags.set(OpenFlags.O_RDONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      try {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[FILE_LEN]);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buffer.array()));
        buffer = ByteBuffer.wrap(new byte[FILE_LEN]);
        Assert.assertEquals(FILE_LEN,
            mFuseFileSystem.read(testFile, buffer, FILE_LEN, FILE_LEN, info));
        for (byte cur : buffer.array()) {
          Assert.assertEquals((byte) 0, cur);
        }
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      }
    }
  }

  /**
   * Tests opening file with O_RDWR on non-existing file for write-only workloads.
   */
  @Test
  public void openReadWriteNonExisting() throws Exception {
    String testFile = "/openReadWriteNonExistingFile";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      info.flags.set(OpenFlags.O_RDWR.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      try {
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info));
        buffer.clear();
        Assert.assertTrue(mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      }
      readAndValidateTestFile(testFile, info, FILE_LEN);
    }
  }

  /**
   * Tests opening file with O_RDWR on existing file for read-only workloads.
   */
  @Test
  public void openReadWriteExisting() throws Exception {
    String testFile = "/openReadWriteExisting";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      createTestFile(testFile, info, FILE_LEN);

      info.flags.set(OpenFlags.O_RDWR.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      try {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[FILE_LEN]);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buffer.array()));
        Assert.assertTrue(mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      }
    }
  }

  /**
   * Tests opening file with O_RDWR on existing empty for write-only workloads.
   */
  @Test
  public void openReadWriteEmptyFile() throws Exception {
    String testFile = "/openReadWriteEmptyFile";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      // Create empty file
      info.flags.set(OpenFlags.O_WRONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.create(testFile, 100644, info));
      Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      // Open empty file for write
      info.flags.set(OpenFlags.O_RDWR.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      try {
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info));
        buffer.clear();
        Assert.assertTrue(mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      }
      readAndValidateTestFile(testFile, info, FILE_LEN);
    }
  }

  /**
   * Tests opening file with O_RDWR and O_TRUNC flags on existing file
   * for write-only workloads.
   */
  @Test
  public void openReadWriteTruncExisting() throws Exception {
    String testFile = "/openReadWriteTruncExisting";
    try (CloseableFuseFileInfo closeableFuseFileInfo = new CloseableFuseFileInfo()) {
      FuseFileInfo info = closeableFuseFileInfo.get();
      createTestFile(testFile, info, FILE_LEN / 2);

      info.flags.set(OpenFlags.O_RDWR.intValue() | OpenFlags.O_TRUNC.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
      try {
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info));
        buffer.clear();
        Assert.assertTrue(mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
      }
      readAndValidateTestFile(testFile, info, FILE_LEN);
    }
  }

  private void createTestFile(String testFile, FuseFileInfo info, int fileLen) {
    info.flags.set(OpenFlags.O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFileSystem.create(testFile, 100644, info));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(fileLen);
    try {
      Assert.assertEquals(fileLen, mFuseFileSystem.write(testFile, buffer, fileLen, 0, info));
    } finally {
      Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
    }
  }

  private void readAndValidateTestFile(String testFile, FuseFileInfo info, int fileLen) {
    info.flags.set(OpenFlags.O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFileSystem.open(testFile, info));
    try {
      ByteBuffer buffer = ByteBuffer.wrap(new byte[fileLen]);
      Assert.assertEquals(fileLen, mFuseFileSystem.read(testFile, buffer, fileLen, 0, info));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileLen, buffer.array()));
    } finally {
      Assert.assertEquals(0, mFuseFileSystem.release(testFile, info));
    }
  }
}
