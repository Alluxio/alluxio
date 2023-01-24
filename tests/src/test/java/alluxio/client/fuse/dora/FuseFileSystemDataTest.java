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

package alluxio.client.fuse.dora;

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
    String path = "/createCloseDifferentThread";
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path, DEFAULT_MODE.toShort(), mFileInfo.get()));
    Callable<Integer> releaseTask = () -> mFuseFs.release(path, mFileInfo.get());
    ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
    Future<Integer> releaseResult = threadExecutor.submit(releaseTask);
    Assert.assertEquals(0, (int) releaseResult.get());

    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(path, mFileInfo.get()));
    releaseResult = threadExecutor.submit(releaseTask);
    Assert.assertEquals(0, (int) releaseResult.get());
  }

  @Test
  public void writeEmptyNonExisting() {
    String path = "/writeEmptyNonExisting";
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.getattr(path, mFileStat));
      Assert.assertEquals(0, mFileStat.st_size.longValue());
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, false, path);
  }

  @Test
  public void readWriteEmptyNonExisting() {
    String path = "/readWriteEmptyNonExisting";
    mFileInfo.get().flags.set(O_RDWR.intValue());
    Assert.assertEquals(0, mFuseFs.open(path, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.getattr(path, mFileStat));
  }

  @Test
  public void overwriteEmpty() {
    String path = "/overwriteEmpty";
    createOpenTest(createOrOpenOperation -> {
      createEmptyFile(path);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN,
          mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.getattr(path, mFileStat));
      Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, true, path);
  }

  @Test
  public void failedToOverwrite() {
    String path = "/failedToOverwrite";
    createOpenTest(createOrOpenOperation -> {
      createFile(path, DEFAULT_FILE_LEN);
      mFileInfo.get().flags.set(O_WRONLY.intValue());
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(-ErrorCodes.EEXIST(),
          mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, true, path);
  }

  @Test
  public void overwriteTruncateFlag() {
    String path = "/overwriteTruncateFlag";
    createOpenTest(createOrOpenOperation -> {
      createFile(path, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(O_TRUNC.intValue()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN,
          mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, true, path);
  }

  @Test
  public void overwriteTruncateZero() {
    String path = "/overwriteTruncateZero";
    createOpenTest(createOrOpenOperation -> {
      createFile(path, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Assert.assertEquals(0, mFuseFs.truncate(path, 0));
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN * 2);
      Assert.assertEquals(DEFAULT_FILE_LEN * 2,
          mFuseFs.write(path, buffer, DEFAULT_FILE_LEN * 2, 0, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      Assert.assertEquals(0,
          mFuseFs.getattr(path, mFileStat));
      Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, true, path);
  }

  @Test
  public void truncateSame() {
    String path = "/truncateSame";
    createOpenTest(createOrOpenOperation -> {
      createFile(path, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      // truncate to original length is no-op
      Assert.assertEquals(0, mFuseFs.truncate(path, DEFAULT_FILE_LEN));
      Assert.assertEquals(0,
          mFuseFs.getattr(path, mFileStat));
      Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
      Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, true, path);
  }

  @Test
  public void truncateMiddle() {
    String path = "/truncateMiddle";
    createOpenTest(createOrOpenOperation -> {
      createFile(path, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        Assert.assertEquals(-ErrorCodes.EOPNOTSUPP(), mFuseFs.truncate(path, DEFAULT_FILE_LEN / 2));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(path, mFileInfo.get());
        mFuseFs.unlink(path);
      }
    }, true, path);
  }

  @Test
  public void overwriteTruncateFuture() {
    String path = "/overwriteTruncateFuture";
    createOpenTest(createOrOpenOperation -> {
      createFile(path, DEFAULT_FILE_LEN);
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        Assert.assertEquals(-ErrorCodes.EOPNOTSUPP(), mFuseFs.truncate(path, DEFAULT_FILE_LEN * 2));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(path, mFileInfo.get());
        mFuseFs.unlink(path);
      }
    }, true, path);
  }

  @Test
  public void writeTruncateFuture() {
    String path = "/writeTruncateFuture";
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(path, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
        Assert.assertEquals(0, mFuseFs.truncate(path, DEFAULT_FILE_LEN * 2));
        Assert.assertEquals(0,
            mFuseFs.getattr(path, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
        buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN * 2);
        Assert.assertEquals(DEFAULT_FILE_LEN * 2,
            mFuseFs.write(path, buffer, DEFAULT_FILE_LEN * 2, DEFAULT_FILE_LEN, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(path, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN * 3, mFileStat.st_size.intValue());
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(path, mFileInfo.get());
        mFuseFs.unlink(path);
      }
    }, true, path);
  }

  @Test
  public void sequentialWrite() {
    String path = "/sequentialWrite";
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(path, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
        buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN, DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, DEFAULT_FILE_LEN, mFileInfo.get()));
        Assert.assertEquals(0,
            mFuseFs.getattr(path, mFileStat));
        Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(path, mFileInfo.get());
        mFuseFs.unlink(path);
      }
    }, true, path);
  }

  @Test
  public void randomWrite() {
    String path = "/randomWrite";
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(-ErrorCodes.EOPNOTSUPP(),
            mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 15, mFileInfo.get()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mFuseFs.release(path, mFileInfo.get());
        mFuseFs.unlink(path);
      }
    }, true, path);
  }

  @Test
  public void openReadEmpty() {
    String path = "/openReadEmpty";
    createEmptyFile(path);
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(path, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
  }

  @Test
  public void openReadNonExisting() {
    String path = "/openReadNonExisting";
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(-ErrorCodes.ENOENT(), mFuseFs.open(path, mFileInfo.get()));
  }

  @Test
  public void openWithLengthLimit() {
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    assertEquals(-ErrorCodes.ENAMETOOLONG(),
        mFuseFs.create(EXCEED_LENGTH_PATH_NAME, DEFAULT_MODE.toShort(), mFileInfo.get()));
  }

  @Test
  public void readTimeoutWhenIncomplete() {
    String path = "/readTimeoutWhenIncomplete";
    createOpenTest(createOrOpenOperation -> {
      try {
        Assert.assertEquals(0, (int) createOrOpenOperation.apply(0));
        ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
        Assert.assertEquals(DEFAULT_FILE_LEN,
            mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
        mFileInfo.get().flags.set(O_RDONLY.intValue());
        Assert.assertEquals(-ErrorCodes.ETIME(), mFuseFs.open(path, mFileInfo.get()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
      }
      Assert.assertEquals(0, mFuseFs.unlink(path));
    }, true, path);
  }

  @Test
  public void writeThenRead() {
    String path = "/writeThenRead";
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
    // read from file
    mFileInfo.get().flags.set(O_RDONLY.intValue());
    Assert.assertEquals(0, mFuseFs.open(path, mFileInfo.get()));
    buffer.flip();
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.read(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    BufferUtils.equalIncreasingByteBuffer(0, DEFAULT_FILE_LEN, buffer);
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
  }

  @Test
  public void readingWhenWriting() {
    String path = "/readingWhenWriting";
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    try {
      ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
      Assert.assertEquals(DEFAULT_FILE_LEN,
          mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));

      mFileInfo.get().flags.set(O_RDONLY.intValue());
      Assert.assertEquals(-ErrorCodes.ETIME(), mFuseFs.open(path, mFileInfo.get()));
    } finally {
      mFuseFs.release(path, mFileInfo.get());
    }
  }

  @Test
  public void getAttrWhenWriting() {
    String path = "/getAttrWhenWriting";
    mFileInfo.get().flags.set(O_WRONLY.intValue());
    Assert.assertEquals(0, mFuseFs.create(path,
        DEFAULT_MODE.toShort(), mFileInfo.get()));
    ByteBuffer buffer = BufferUtils.getIncreasingByteBuffer(DEFAULT_FILE_LEN);
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(path, mFileStat));
    Assert.assertEquals(DEFAULT_FILE_LEN, mFileStat.st_size.intValue());
    Assert.assertEquals(AlluxioFuseUtils.getSystemUid(), mFileStat.st_uid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), mFileStat.st_gid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), mFileStat.st_gid.get());
    Assert.assertTrue((mFileStat.st_mode.intValue() & FileStat.S_IFREG) != 0);
    buffer.flip();
    Assert.assertEquals(DEFAULT_FILE_LEN,
        mFuseFs.write(path, buffer, DEFAULT_FILE_LEN, 0, mFileInfo.get()));
    Assert.assertEquals(0,
        mFuseFs.getattr(path, mFileStat));
    Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
    Assert.assertEquals(0, mFuseFs.release(path, mFileInfo.get()));
    Assert.assertEquals(DEFAULT_FILE_LEN * 2, mFileStat.st_size.intValue());
    Assert.assertEquals(0, mFuseFs.unlink(path));
  }

  private void createOpenTest(Consumer<Function<Integer, Integer>> testCase,
      boolean testOpenReadWrite, String path) {
    List<Function<Integer, Integer>> operations = new ArrayList<>();
    operations.add(extraFlag -> {
      mFileInfo.get().flags.set(O_WRONLY.intValue() | extraFlag);
      return mFuseFs.open(path, mFileInfo.get());
    });
    operations.add(extraFlag -> {
      mFileInfo.get().flags.set(O_WRONLY.intValue() | extraFlag);
      return mFuseFs.create(path, DEFAULT_MODE.toShort(), mFileInfo.get());
    });
    if (testOpenReadWrite) {
      operations.add(extraFlag -> {
        mFileInfo.get().flags.set(O_RDWR.intValue() | extraFlag);
        return mFuseFs.open(path, mFileInfo.get());
      });
    }
    for (Function<Integer, Integer> ops : operations) {
      testCase.accept(ops);
    }
  }
}
