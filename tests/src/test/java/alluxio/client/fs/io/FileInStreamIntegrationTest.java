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

package alluxio.client.fs.io;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileBlockInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for {@link alluxio.client.file.FileInStream}.
 */
public final class FileInStreamIntegrationTest extends BaseIntegrationTest {
  // The block size needs to be sufficiently large based on TCP send/receive buffers, set to 1MB.
  private static final int BLOCK_SIZE = Constants.MB;
  private static final int MIN_LEN = BLOCK_SIZE + 1;
  private static final int MAX_LEN = BLOCK_SIZE * 4 + 1;
  private static final int DELTA = BLOCK_SIZE / 2;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE).build();
  private FileSystem mFileSystem;
  private CreateFilePOptions mWriteBoth;
  private CreateFilePOptions mWriteAlluxio;
  private CreateFilePOptions mWriteUnderStore;
  private String mTestPath;

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = CreateFilePOptions.newBuilder().setMode(Mode.createFullAccess().toProto())
        .setBlockSizeBytes(BLOCK_SIZE).setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build();
    mWriteAlluxio = CreateFilePOptions.newBuilder().setMode(Mode.createFullAccess().toProto())
        .setBlockSizeBytes(BLOCK_SIZE).setWriteType(WritePType.MUST_CACHE).setRecursive(true)
        .build();
    mWriteUnderStore = CreateFilePOptions.newBuilder().setMode(Mode.createFullAccess().toProto())
        .setBlockSizeBytes(BLOCK_SIZE).setWriteType(WritePType.THROUGH).setRecursive(true)
        .build();
    mTestPath = PathUtils.uniqPath();

    // Create files of varying size and write type to later read from
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        AlluxioURI path = new AlluxioURI(mTestPath + "/file_" + k + "_" + op.hashCode());
        FileSystemTestUtils.createByteFile(mFileSystem, path, op, k);
      }
    }
  }

  private List<CreateFilePOptions> getOptionSet() {
    List<CreateFilePOptions> ret = new ArrayList<>(3);
    ret.add(mWriteBoth);
    ret.add(mWriteAlluxio);
    ret.add(mWriteUnderStore);
    return ret;
  }

  /**
   * Tests {@link FileInStream#read()} across block boundary.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB"})
  public void readTest1() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k];
        int value = is.read();
        int cnt = 0;
        while (value != -1) {
          Assert.assertTrue(value >= 0);
          Assert.assertTrue(value < 256);
          ret[cnt++] = (byte) value;
          value = is.read();
        }
        Assert.assertEquals(cnt, k);
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        value = is.read();
        cnt = 0;
        while (value != -1) {
          Assert.assertTrue(value >= 0);
          Assert.assertTrue(value < 256);
          ret[cnt++] = (byte) value;
          value = is.read();
        }
        Assert.assertEquals(cnt, k);
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link FileInStream#read(byte[])}.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB"})
  public void readTest2() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();

        is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link FileInStream#read(byte[], int, int)}.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB"})
  public void readTest3() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        byte[] ret = new byte[k / 2];
        Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k / 2, ret));
        is.close();

        is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        ret = new byte[k];
        Assert.assertEquals(k, is.read(ret, 0, k));
        Assert.assertTrue(BufferUtils.equalIncreasingByteArray(k, ret));
        is.close();
      }
    }
  }

  /**
   * Tests {@link FileInStream#read(byte[], int, int)} for end of file.
   */
  @Test
  public void readEndOfFile() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        try (FileInStream is = mFileSystem
            .openFile(uri, FileSystemTestUtils.toOpenFileOptions(op))) {
          byte[] ret = new byte[k / 2];
          int readBytes = is.read(ret, 0, k / 2);
          while (readBytes != -1) {
            readBytes = is.read(ret);
            Assert.assertTrue(0 != readBytes);
          }
          Assert.assertEquals(-1, readBytes);
        }
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)}. Validate the expected exception for seeking a negative
   * position.
   */
  @Test
  public void seekExceptionTest1() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        try (FileInStream is = mFileSystem
            .openFile(uri, FileSystemTestUtils.toOpenFileOptions(op))) {
          is.seek(-1);
        }
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)}. Validate the expected exception for seeking a position
   * that is past EOF.
   */
  @Test
  public void seekExceptionTest2() throws Exception {
    mThrown.expect(IllegalArgumentException.class);
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        try (FileInStream is = mFileSystem
            .openFile(uri, FileSystemTestUtils.toOpenFileOptions(op))) {
          is.seek(k + 1);
        }
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)}.
   */
  @Test
  public void seek() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        is.seek(k / 3);
        Assert.assertEquals(BufferUtils.intAsUnsignedByteValue(k / 3), is.read());
        is.seek(k / 2);
        Assert.assertEquals(BufferUtils.intAsUnsignedByteValue(k / 2), is.read());
        is.seek(k / 4);
        Assert.assertEquals(BufferUtils.intAsUnsignedByteValue(k / 4), is.read());
        is.close();
      }
    }
  }

  /**
   * Tests {@link FileInStream#seek(long)} when at the end of a file at the block boundary.
   */
  @Test
  public void eofSeek() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int length = BLOCK_SIZE * 3;
    for (CreateFilePOptions op : getOptionSet()) {
      String filename = uniqPath + "/file_" + op.hashCode();
      AlluxioURI uri = new AlluxioURI(filename);
      FileSystemTestUtils.createByteFile(mFileSystem, filename, length, op);

      FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
      byte[] data = new byte[length];
      is.read(data, 0, length);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(length, data));
      is.seek(0);
      is.read(data, 0, length);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(length, data));
      is.close();
    }
  }

  /**
   * Tests {@link FileInStream#skip(long)}.
   */
  @Test
  public void skip() throws Exception {
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      for (CreateFilePOptions op : getOptionSet()) {
        String filename = mTestPath + "/file_" + k + "_" + op.hashCode();
        AlluxioURI uri = new AlluxioURI(filename);

        FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
        Assert.assertEquals(k / 2, is.skip(k / 2));
        Assert.assertEquals(BufferUtils.intAsUnsignedByteValue(k / 2), is.read());
        Assert.assertEquals(k / 3, is.skip(k / 3));
        // position is k / 2 (skip) + k / 3 (skip) + 1 (read)
        Assert.assertEquals(BufferUtils.intAsUnsignedByteValue(k / 2 + k / 3 + 1), is.read());
        is.close();
      }
    }
  }

  /**
   * Tests when there are multiple readers reading the same file that is in UFS.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "false",
          PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "10240",
          PropertyKey.Name.USER_FILE_BUFFER_BYTES, "128"})
  public void concurrentRemoteRead() throws Exception {
    int blockSize = 10240;
    final int bufferSize = 128;
    final int length = blockSize * 2;

    // Create files of varying size and write type to later read from
    final AlluxioURI path = new AlluxioURI(mTestPath + "/largeFile");
    FileSystemTestUtils.createByteFile(mFileSystem, path,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).build(), length);

    final int concurrency = 10;
    final AtomicInteger count = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(concurrency * 2);
    for (int i = 0; i < concurrency; ++i) {
      service.submit(new Runnable() {
        @Override
        public void run() {
          try (FileInStream is = mFileSystem.openFile(path,
              OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
            int start = 0;
            while (start < length) {
              byte[] buffer = new byte[bufferSize];
              int bytesRead = is.read(buffer, 0, bufferSize);
              Assert.assertTrue(BufferUtils.equalIncreasingByteArray(start, bytesRead, buffer));
              start = bytesRead + start;
            }
            count.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          try (FileInStream is = mFileSystem.openFile(path,
              OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build())) {
            int start = 0;
            while (start < length) {
              byte[] buffer = new byte[bufferSize];
              int bytesRead = is.read(buffer, 0, bufferSize);
              Assert.assertTrue(BufferUtils.equalIncreasingByteArray(start, bytesRead, buffer));
              start = bytesRead + start;
            }
            count.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    service.shutdown();
    service.awaitTermination(Constants.MINUTE_MS, TimeUnit.MILLISECONDS);
    Assert.assertEquals(concurrency * 2, count.get());
  }

  /**
   * Read large file remotely. Make sure the test does not timeout.
   */
  @Test(timeout = 30000)
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_SHORT_CIRCUIT_ENABLED, "false",
          PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "16MB",
          PropertyKey.Name.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64KB",
          PropertyKey.Name.WORKER_RAMDISK_SIZE, "1GB"})
  public void remoteReadLargeFile() throws Exception {
    // write a file outside of Alluxio
    AlluxioURI filePath = new AlluxioURI(mTestPath + "/test");
    try (FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(16 * Constants.MB).setWriteType(WritePType.THROUGH).build())) {
      // Write a smaller byte array 10 times to avoid demanding 500mb of contiguous memory.
      byte[] bytes = BufferUtils.getIncreasingByteArray(50 * Constants.MB);
      for (int i = 0; i < 10; i++) {
        os.write(bytes);
      }
    }

    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    try (FileInStream in = mFileSystem.openFile(filePath, options)) {
      byte[] buf = new byte[8 * Constants.MB];
      while (in.read(buf) != -1) {
      }
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_FILE_READ_TYPE_DEFAULT, "NO_CACHE"})
  public void positionedReadWithoutCaching() throws Exception {
    for (CreateFilePOptions op : getOptionSet()) {
      String filename = mTestPath + "/file_" + MIN_LEN + "_" + op.hashCode();
      AlluxioURI uri = new AlluxioURI(filename);

      FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op));
      byte[] ret = new byte[DELTA - 1];
      Assert.assertEquals(DELTA - 1, is.positionedRead(MIN_LEN - DELTA + 1, ret, 0, DELTA));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(MIN_LEN - DELTA + 1, DELTA - 1, ret));
      is.close();
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_FILE_SEQUENTIAL_PREAD_THRESHOLD, "700KB"})
  public void positionedReadWithLargeThreshold() throws Exception {
    List<CreateFilePOptions> optionSet = new ArrayList<>(2);
    optionSet.add(mWriteBoth);
    optionSet.add(mWriteUnderStore);
    for (CreateFilePOptions op : optionSet) {
      String filename = mTestPath + "/file_" + MIN_LEN + "_" + op.hashCode();
      AlluxioURI uri = new AlluxioURI(filename);

      try (FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op))) {
        byte[] ret = new byte[DELTA - 1];
        Assert.assertEquals(DELTA - 1,
            is.positionedRead(MIN_LEN - DELTA + 1, ret, 0, DELTA));
        Assert.assertTrue(
            BufferUtils.equalIncreasingByteArray(MIN_LEN - DELTA + 1, DELTA - 1, ret));
      }
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_FILE_SEQUENTIAL_PREAD_THRESHOLD, "200KB"})
  public void positionedReadWithSmallThreshold() throws Exception {
    List<CreateFilePOptions> optionSet = new ArrayList<>(2);
    optionSet.add(mWriteBoth);
    optionSet.add(mWriteUnderStore);
    for (CreateFilePOptions op : optionSet) {
      String filename = mTestPath + "/file_" + MIN_LEN + "_" + op.hashCode();
      AlluxioURI uri = new AlluxioURI(filename);

      try (FileInStream is = mFileSystem.openFile(uri, FileSystemTestUtils.toOpenFileOptions(op))) {
        byte[] ret = new byte[DELTA - 1];
        Assert.assertEquals(DELTA - 1,
            is.positionedRead(MIN_LEN - DELTA + 1, ret, 0, DELTA));
        Assert.assertTrue(
            BufferUtils.equalIncreasingByteArray(MIN_LEN - DELTA + 1, DELTA - 1, ret));
      }
    }
  }

  @Test(timeout = 10000)
  public void asyncCacheFirstBlock() throws Exception {
    String filename = mTestPath + "/file_" + MAX_LEN + "_" + mWriteUnderStore.hashCode();
    AlluxioURI uri = new AlluxioURI(filename);

    for (ReadType readType : ReadType.values()) {
      mFileSystem.free(uri);
      CommonUtils.waitFor("No in-Alluxio data left from previous iteration.", () -> {
        try {
          URIStatus st = mFileSystem.getStatus(uri);
          return st.getInAlluxioPercentage() == 0;
        } catch (Exception e) {
          return false;
        }
      });
      FileInStream is = mFileSystem.openFile(uri,
          OpenFilePOptions.newBuilder().setReadType(readType.toProto()).build());
      is.read();
      URIStatus status = mFileSystem.getStatus(uri);
      Assert.assertEquals(0, status.getInAlluxioPercentage());
      is.close();
      if (readType.isCache()) {
        CommonUtils.waitFor("First block to be cached.", () -> {
          try {
            URIStatus st = mFileSystem.getStatus(uri);
            boolean achieved = true;
            // Expect only first block to be cached, other blocks should be empty in Alluxio
            for (int i = 0; i < st.getFileBlockInfos().size(); i++) {
              FileBlockInfo info = st.getFileBlockInfos().get(i);
              if (i == 0) {
                achieved = achieved && !info.getBlockInfo().getLocations().isEmpty();
              } else {
                achieved = achieved && info.getBlockInfo().getLocations().isEmpty();
              }
            }
            return achieved;
          } catch (Exception e) {
            return false;
          }
        });
      } else {
        Thread.sleep(1000);
        status = mFileSystem.getStatus(uri);
        Assert.assertEquals(0, status.getInAlluxioPercentage());
      }
    }
  }

  @Test(timeout = 10000)
  public void asyncCacheAfterSeek() throws Exception {
    String filename = mTestPath + "/file_" + MAX_LEN + "_" + mWriteUnderStore.hashCode();
    AlluxioURI uri = new AlluxioURI(filename);

    for (ReadType readType : ReadType.values()) {
      mFileSystem.free(uri);
      CommonUtils.waitFor("No in-Alluxio data left from previous iteration.", () -> {
        try {
          URIStatus st = mFileSystem.getStatus(uri);
          return st.getInAlluxioPercentage() == 0;
        } catch (Exception e) {
          return false;
        }
      });
      FileInStream is = mFileSystem.openFile(uri,
          OpenFilePOptions.newBuilder().setReadType(readType.toProto()).build());
      URIStatus status = mFileSystem.getStatus(uri);
      is.seek(status.getBlockSizeBytes() + 1);
      is.read();
      status = mFileSystem.getStatus(uri);
      Assert.assertEquals(0, status.getInAlluxioPercentage());
      is.close();
      if (readType.isCache()) {
        CommonUtils.waitFor("Second block to be cached.", () -> {
          try {
            URIStatus st = mFileSystem.getStatus(uri);
            boolean achieved = true;
            // Expect only second block to be cached, other blocks should be empty in Alluxio
            for (int i = 0; i < st.getFileBlockInfos().size(); i++) {
              FileBlockInfo info = st.getFileBlockInfos().get(i);
              if (i == 1) {
                achieved = achieved && !info.getBlockInfo().getLocations().isEmpty();
              } else {
                achieved = achieved && info.getBlockInfo().getLocations().isEmpty();
              }
            }
            return achieved;
          } catch (Exception e) {
            return false;
          }
        });
      } else {
        Thread.sleep(1000);
        status = mFileSystem.getStatus(uri);
        Assert.assertEquals(0, status.getInAlluxioPercentage());
      }
    }
  }

  @Test(timeout = 10000)
  public void asyncCacheFirstBlockPRead() throws Exception {
    String filename = mTestPath + "/file_" + MAX_LEN + "_" + mWriteUnderStore.hashCode();
    AlluxioURI uri = new AlluxioURI(filename);

    for (ReadType readType : ReadType.values()) {
      mFileSystem.free(uri);
      CommonUtils.waitFor("No in-Alluxio data left from previous iteration.", () -> {
        try {
          URIStatus st = mFileSystem.getStatus(uri);
          return st.getInAlluxioPercentage() == 0;
        } catch (Exception e) {
          return false;
        }
      });
      FileInStream is = mFileSystem.openFile(uri,
          OpenFilePOptions.newBuilder().setReadType(readType.toProto()).build());
      // Positioned reads trigger async caching after reading and do not need to wait for a close
      // or a block boundary to be crossed.
      URIStatus status = mFileSystem.getStatus(uri);
      Assert.assertEquals(0, status.getInAlluxioPercentage());
      is.positionedRead(BLOCK_SIZE / 2, new byte[1], 0, 1);
      if (readType.isCache()) {
        CommonUtils.waitFor("First block to be cached.", () -> {
          try {
            URIStatus st = mFileSystem.getStatus(uri);
            boolean achieved = true;
            // Expect only first block to be cached, other blocks should be empty in Alluxio
            for (int i = 0; i < st.getFileBlockInfos().size(); i++) {
              FileBlockInfo info = st.getFileBlockInfos().get(i);
              if (i == 0) {
                achieved = achieved && !info.getBlockInfo().getLocations().isEmpty();
              } else {
                achieved = achieved && info.getBlockInfo().getLocations().isEmpty();
              }
            }
            return achieved;
          } catch (Exception e) {
            return false;
          }
        });
      } else {
        Thread.sleep(1000);
        status = mFileSystem.getStatus(uri);
        Assert.assertEquals(0, status.getInAlluxioPercentage());
      }
      is.close();
    }
  }

  @Test
  public void syncCacheFirstBlock() throws Exception {
    String filename = mTestPath + "/file_" + MAX_LEN + "_" + mWriteUnderStore.hashCode();
    AlluxioURI uri = new AlluxioURI(filename);

    FileInStream is = mFileSystem.openFile(uri,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build());
    URIStatus status = mFileSystem.getStatus(uri);
    byte[] data = new byte[(int) status.getBlockSizeBytes() + 1];
    is.read(data);
    status = mFileSystem.getStatus(uri);
    Assert.assertFalse(status.getFileBlockInfos().get(0).getBlockInfo().getLocations().isEmpty());
    is.close();
  }
}
