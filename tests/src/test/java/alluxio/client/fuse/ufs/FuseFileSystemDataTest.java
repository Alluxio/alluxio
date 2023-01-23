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

import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_RDWR;
import static jnr.constants.platform.OpenFlags.O_TRUNC;
import static jnr.constants.platform.OpenFlags.O_WRONLY;
import static org.junit.Assert.assertEquals;

import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.struct.FileStat;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Isolation tests for {@link alluxio.fuse.AlluxioJniFuseFileSystem} with local UFS.
 * This test covers the basic file system read write operations.
 */
public class FuseFileSystemDataTest extends AbstractFuseFileSystemTest {
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
  public void writeEmptyNonExisting() {
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
      Assert.assertEquals(0, mFileStat.st_size.longValue());
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, false);
  }

  @Test
  public void readWriteEmptyNonExisting() {
    mFileInfo.get().flags.set(O_RDWR.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(FILE, mFileStat));
  }

  @Test
  public void overwriteEmpty() {
    createOpenTest(createOrOpenOperation -> {
      createEmptyFile(FILE);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN,
          mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.getattr(FILE, mFileStat));
      Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, true);
  }

  @Test
  public void failedToOverwrite() {
    createOpenTest(createOrOpenOperation -> {
      createFile(FILE, DEFAULT_FILE_LEN);
      mFileInfo.get().flags.set(O_WRONLY.intValue());
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(-ErrorCodes.EEXIST(),
          mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, true);
  }

  @Test
  public void overwriteTruncateFlag() {
    createOpenTest(createOrOpenOperation -> {
      createFile(FILE, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(O_TRUNC.intValue()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN,
          mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, true);
  }

  @Test
  public void overwriteTruncateZero() {
    createOpenTest(createOrOpenOperation -> {
      createFile(FILE, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Assert.assertEquals(0, mFuseFs.truncate(FILE, 0));
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2,
          mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN * 2, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      Assert.assertEquals(0,
          mFuseFs.getattr(FILE, mFileStat));
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, true);
  }

  @Test
  public void truncateSame() {
    createOpenTest(createOrOpenOperation -> {
      createFile(FILE, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      // truncate to original length is no-op
      Assert.assertEquals(0, mFuseFs.truncate(FILE, DEFAULT_FILE_LEN));
      Assert.assertEquals(0,
          mFuseFs.getattr(FILE, mFileStat));
      Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
      Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, true);
  }

  @Test
  public void truncateMiddle() {
    createOpenTest(createOrOpenOperation -> {
      createFile(FILE, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        Assert.assertEquals(-ErrorCodes.EOPNOTSUPP(), mFuseFs.truncate(FILE, DEFAULT_FILE_LEN / 2));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(FILE, mFileInfo.get());
        mFuseFs.unlink(FILE);
      }
    }, true);
  }

  @Test
  public void overwriteTruncateFuture() {
    createOpenTest(createOrOpenOperation -> {
      createFile(FILE, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        Assert.assertEquals(-ErrorCodes.EOPNOTSUPP(), mFuseFs.truncate(FILE, DEFAULT_FILE_LEN * 2));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(FILE, mFileInfo.get());
        mFuseFs.unlink(FILE);
      }
    }, true);
  }

  @Test
  public void writeTruncateFuture() {
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(FILE, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
        Assert.assertEquals(0, mFuseFs.truncate(FILE, DEFAULT_FILE_LEN * 2));
        Assert.assertEquals(0,
            mFuseFs.getattr(FILE, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
        buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN * 2);
        Assert.assertEquals(DEFAULT_FILE_LEN * 2,
            mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN * 2, DEFAULT_FILE_LEN, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(FILE, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN * 3, mFileStat.st_size.intValue());
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(FILE, mFileInfo.get());
        mFuseFs.unlink(FILE);
      }
    }, true);
  }

  @Test
  public void sequentialWrite() {
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(FILE, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
        buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(FILE, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(FILE, mFileInfo.get());
        mFuseFs.unlink(FILE);
      }
    }, true);
  }

  @Test
  public void randomWrite() {
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(-ErrorCodes.EOPNOTSUPP(),
            mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 15, mFileInfo.get()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(FILE, mFileInfo.get());
        mFuseFs.unlink(FILE);
      }
    }, true);
  }

  @Test
  public void openReadEmpty() {
    createEmptyFile(FILE);
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
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
  public void readTimeoutWhenIncomplete() {
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
        mFileInfo.get().flags.set(O_RDONLY.intValue());
        Assert.assertEquals(-ErrorCodes.ETIME(), mFuseFs.open(FILE, mFileInfo.get()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
      }
      Assert.assertEquals(0, mFuseFs.unlink(FILE));
    }, true);
  }

  @Test
  public void writeThenRead() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    // read from file
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(FILE, mFileInfo.get()));
    buffer.flip();
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.read(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, buffer);
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
  }

  @Test
  public void readingWhenWriting() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(FILE,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    try {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN,
          mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));

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
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
    Assert.assertEquals(AlluxioFuseUtils.getSystemUid(), mFileStat.st_uid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), mFileStat.st_gid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), mFileStat.st_gid.get());
    Assert.assertTrue((mFileStat.st_mode.intValue() & FileStat.S_IFREG) != 0);
    buffer.flip();
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.write(FILE, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(FILE, mFileStat));
    Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
    Assert.assertEquals(0, mFuseFs.release(FILE, mFileInfo.get()));
    Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
    Assert.assertEquals(0, mFuseFs.unlink(FILE));
  }

  private void createOpenTest(Consumer<Function<Integer, Integer>> testCase,
      boolean testOpenReadWrite) {
    List<Function<Integer, Integer>> operations = new ArrayList<>();
    operations.add(extraFlag -> {
      mFileInfo.get().flags.set(O_WRONLY.intValue() | extraFlag);
      return mFuseFs.open(FILE, mFileInfo.get());
    });
    operations.add(extraFlag -> {
      mFileInfo.get().flags.set(O_WRONLY.intValue() | extraFlag);
      return mFuseFs.create(FILE, DEFAULT_MODE.toShort(), mFileInfo.get());
    });
    if (testOpenReadWrite) {
      operations.add(extraFlag -> {
        mFileInfo.get().flags.set(O_RDWR.intValue() | extraFlag);
        return mFuseFs.open(FILE, mFileInfo.get());
      });
    }
    for (Function<Integer, Integer> ops : operations) {
      testCase.accept(ops);
    }
  }
}
