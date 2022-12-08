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
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.options.FuseOptions;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.utils.NativeLibraryLoader;
import alluxio.resource.CloseableResource;
import alluxio.util.io.BufferUtils;

import jnr.constants.platform.OpenFlags;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

/**
 * Integration tests for JNR-FUSE based {@link AlluxioJniFuseFileSystem}.
 */
public class JNIFuseIntegrationTest extends AbstractFuseIntegrationTest {
  private static final int FILE_LEN = 128;

  static {
    NativeLibraryLoader.getInstance().loadLibfuse(
        AlluxioFuseUtils.getAndCheckLibfuseVersion(Configuration.global()));
  }

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
    mFuseFileSystem = new AlluxioJniFuseFileSystem(context, fileSystem,
        FuseOptions.create(conf));
    mFuseFileSystem.mount(false, false, new HashSet<>());
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
  public void createWriteOpenRead() {
    String testFile = "/createWriteOpenReadTestFile";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
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
  public void openWriteNonExisting() {
    String testFile = "/openWriteNonExisting";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      info.get().flags.set(OpenFlags.O_WRONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
      try {
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .write(testFile, buffer, FILE_LEN, 0, info.get()));
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      }
      readAndValidateTestFile(testFile, info.get(), FILE_LEN);
    }
  }

  /**
   * Tests opening a file with O_WRONLY on existing file without O_TRUNC.
   */
  @Test
  public void openWriteExistingWithoutTruncFlag() {
    String testFile = "/openWriteExistingWithoutTruncFlag";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      createTestFile(testFile, info.get(), FILE_LEN);

      info.get().flags.set(OpenFlags.O_WRONLY.intValue());
      try {
        Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        // O_WRONLY without O_TRUNC cannot overwrite
        Assert.assertTrue(mFuseFileSystem.write(testFile, buffer, FILE_LEN, 0, info.get()) < 0);
      } finally {
        mFuseFileSystem.release(testFile, info.get());
      }
    }
  }

  /**
   * Tests opening a file with O_WRONLY and O_TRUNC flags to overwrite existing file.
   */
  @Test
  public void openWriteExistingWithTruncFlag() {
    String testFile = "/openWriteExistingWithTruncFlag";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      createTestFile(testFile, info.get(), FILE_LEN / 2);

      info.get().flags.set(OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue());
      try {
        Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem.write(testFile,
            buffer, FILE_LEN, 0, info.get()));
      } finally {
        mFuseFileSystem.release(testFile, info.get());
      }
      readAndValidateTestFile(testFile, info.get(), FILE_LEN);
    }
  }

  /**
   * Tests opening a file with O_WRONLY and then truncating it to zero
   * length for writing.
   */
  @Test
  public void openWriteExistingWithTruncate() {
    String testFile = "/openWriteExistingWithTruncate";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      createTestFile(testFile, info.get(), FILE_LEN / 2);

      info.get().flags.set(OpenFlags.O_WRONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      try {
        // truncate to original length is no-op
        Assert.assertEquals(0, mFuseFileSystem.truncate(testFile, FILE_LEN / 2));
        // truncate to a large value
        Assert.assertNotEquals(0, mFuseFileSystem.truncate(testFile, FILE_LEN));
        // delete file
        Assert.assertEquals(0, mFuseFileSystem.truncate(testFile, 0));
        Assert.assertEquals(0, mFuseFileSystem.truncate(testFile, FILE_LEN * 2));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .write(testFile, buffer, FILE_LEN, 0, info.get()));
      } finally {
        mFuseFileSystem.release(testFile, info.get());
      }
      info.get().flags.set(OpenFlags.O_RDONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      try {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[FILE_LEN]);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .read(testFile, buffer, FILE_LEN, 0, info.get()));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buffer.array()));
        buffer = ByteBuffer.wrap(new byte[FILE_LEN]);
        Assert.assertEquals(FILE_LEN,
            mFuseFileSystem.read(testFile, buffer, FILE_LEN, FILE_LEN, info.get()));
        for (byte cur : buffer.array()) {
          Assert.assertEquals((byte) 0, cur);
        }
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      }
    }
  }

  /**
   * Tests opening file with O_RDWR on non-existing file for write-only workloads.
   */
  @Test
  public void openReadWriteNonExisting() {
    String testFile = "/openReadWriteNonExistingFile";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      info.get().flags.set(OpenFlags.O_RDWR.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      try {
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .write(testFile, buffer, FILE_LEN, 0, info.get()));
        buffer.clear();
        Assert.assertTrue(mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info.get()) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      }
      readAndValidateTestFile(testFile, info.get(), FILE_LEN);
    }
  }

  /**
   * Tests opening file with O_RDWR on existing file for read-only workloads.
   */
  @Test
  public void openReadWriteExisting() {
    String testFile = "/openReadWriteExisting";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      createTestFile(testFile, info.get(), FILE_LEN);

      info.get().flags.set(OpenFlags.O_RDWR.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      try {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[FILE_LEN]);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .read(testFile, buffer, FILE_LEN, 0, info.get()));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(FILE_LEN, buffer.array()));
        Assert.assertTrue(mFuseFileSystem
            .write(testFile, buffer, FILE_LEN, 0, info.get()) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      }
    }
  }

  /**
   * Tests opening file with O_RDWR on existing empty for write-only workloads.
   */
  @Test
  public void openReadWriteEmptyFile() {
    String testFile = "/openReadWriteEmptyFile";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      // Create empty file
      info.get().flags.set(OpenFlags.O_WRONLY.intValue());
      Assert.assertEquals(0, mFuseFileSystem.create(testFile, 100644, info.get()));
      Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      // Open empty file for write
      info.get().flags.set(OpenFlags.O_RDWR.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      try {
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .write(testFile, buffer, FILE_LEN, 0, info.get()));
        buffer.clear();
        Assert.assertTrue(mFuseFileSystem
            .read(testFile, buffer, FILE_LEN, 0, info.get()) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      }
      readAndValidateTestFile(testFile, info.get(), FILE_LEN);
    }
  }

  /**
   * Tests opening file with O_RDWR and O_TRUNC flags on existing file
   * for write-only workloads.
   */
  @Test
  public void openReadWriteTruncExisting() {
    String testFile = "/openReadWriteTruncExisting";
    try (CloseableResource<FuseFileInfo> info
        = AlluxioFuseUtils.createTestFuseFileInfo(mFuseFileSystem)) {
      createTestFile(testFile, info.get(), FILE_LEN / 2);

      info.get().flags.set(OpenFlags.O_RDWR.intValue() | OpenFlags.O_TRUNC.intValue());
      Assert.assertEquals(0, mFuseFileSystem.open(testFile, info.get()));
      try {
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(FILE_LEN);
        Assert.assertEquals(FILE_LEN, mFuseFileSystem
            .write(testFile, buffer, FILE_LEN, 0, info.get()));
        buffer.clear();
        Assert.assertTrue(mFuseFileSystem.read(testFile, buffer, FILE_LEN, 0, info.get()) < 0);
      } finally {
        Assert.assertEquals(0, mFuseFileSystem.release(testFile, info.get()));
      }
      readAndValidateTestFile(testFile, info.get(), FILE_LEN);
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
