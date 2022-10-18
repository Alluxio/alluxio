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

package alluxio.client.file.ufs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;

/**
 * Add unit tests for {@link UfsFileInStream}.
 */
public class UfsFileInStreamTest {
  private static final int CHUNK_SIZE = 100;
  private String mRootUfs;
  private UnderFileSystem mUfs;

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    mRootUfs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
    UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
    mUfs = UnderFileSystem.Factory.create(mRootUfs, Configuration.global());
  }

  @After
  public void after() throws IOException, AlluxioException {
    mUfs.deleteDirectory(mRootUfs, DeleteOptions.defaults().setRecursive(true));
  }

  @Test
  public void readWithNullUfsStream() {
    assertThrows(NullPointerException.class,
        () -> new UfsFileInStream(null, 0L).close());
  }

  @Test
  public void openClose() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, 0);
    getStream(ufsPath).close();
  }

  @Test
  public void singleByteRead() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, 1);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(0, inStream.read());
    }
  }

  @Test
  public void twoBytesRead() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, 2);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(0, inStream.read());
      assertEquals(1, inStream.read());
    }
  }

  @Test
  public void manyBytesRead() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      byte[] res = new byte[CHUNK_SIZE];
      assertEquals(CHUNK_SIZE, inStream.read(res));
      assertTrue(BufferUtils.equalIncreasingByteArray(CHUNK_SIZE, res));
    }
  }

  @Test
  public void manyBytesReadByteBuffer() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(CHUNK_SIZE, inStream.read(buffer));
      assertTrue(BufferUtils.equalIncreasingByteBuffer(0, CHUNK_SIZE, buffer));
    }
  }

  @Test
  public void readAll() throws IOException {
    int len = CHUNK_SIZE * 5;
    int start = 0;
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE * 5);
    byte[] res = new byte[CHUNK_SIZE];
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      while (start < len) {
        assertEquals(CHUNK_SIZE, inStream.read(res));
        assertTrue(BufferUtils.equalIncreasingByteArray(start, CHUNK_SIZE, res));
        start += CHUNK_SIZE;
      }
    }
  }

  @Test
  public void readAllByteBuffer() throws IOException {
    int len = CHUNK_SIZE * 5;
    int start = 0;
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE * 5);
    ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      while (start < len) {
        assertEquals(CHUNK_SIZE, inStream.read(buffer));
        assertTrue(BufferUtils.equalIncreasingByteBuffer(start, CHUNK_SIZE, buffer));
        start += CHUNK_SIZE;
        buffer.clear();
      }
    }
  }

  @Test
  public void readOffset() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    int start = CHUNK_SIZE / 4;
    int len = CHUNK_SIZE / 2;
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      byte[] res = new byte[CHUNK_SIZE];
      assertEquals(CHUNK_SIZE / 2, inStream.read(res, start, len));
      for (int i = start; i < start + len; i++) {
        assertEquals(i - start, res[i]);
      }
    }
  }

  @Test
  public void readOffsetByteBuffer() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    int start = CHUNK_SIZE / 4;
    int len = CHUNK_SIZE / 2;
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
      assertEquals(CHUNK_SIZE / 2, inStream.read(buffer, start, len));
      for (int i = start; i < start + len; i++) {
        assertEquals(i - start, buffer.get(i));
      }
    }
  }

  @Test
  public void readOutOfBound() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      byte[] res = new byte[CHUNK_SIZE * 2];
      assertEquals(CHUNK_SIZE, inStream.read(res));
      assertTrue(BufferUtils.matchIncreasingByteArray(0, CHUNK_SIZE, res));
      assertEquals(-1, inStream.read(res));
    }
  }

  @Test
  public void readOutOfBoundByteBuffer() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE * 2);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(CHUNK_SIZE, inStream.read(buffer));
      assertTrue(BufferUtils.matchIncreasingByteBuffer(0, CHUNK_SIZE, buffer));
      assertEquals(-1, inStream.read(buffer));
    }
  }

  @Test
  public void readOverflowOffLen() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(IllegalArgumentException.class,
          () -> inStream.read(new byte[CHUNK_SIZE], 0, CHUNK_SIZE * 2));
    }
  }

  @Test
  public void readOverflowOffLenByteBuffer() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(IllegalArgumentException.class,
          () -> inStream.read(ByteBuffer.allocate(CHUNK_SIZE), 0, CHUNK_SIZE * 2));
    }
  }

  @Test
  public void readNullArray() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(NullPointerException.class,
          () -> inStream.read((byte[]) null));
    }
  }

  @Test
  public void readNullBuffer() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(NullPointerException.class,
          () -> inStream.read((ByteBuffer) null, 0, CHUNK_SIZE));
    }
  }

  @Test
  public void readNullArrayOffset() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(NullPointerException.class,
          () -> inStream.read((byte[]) null, 0, CHUNK_SIZE));
    }
  }

  @Test
  public void readNullBufferOffset() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(NullPointerException.class,
          () -> inStream.read((ByteBuffer) null, 0, CHUNK_SIZE));
    }
  }

  @Test
  public void positionedRead() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      byte[] res = new byte[CHUNK_SIZE / 2];
      assertEquals(CHUNK_SIZE / 2,
          inStream.positionedRead(CHUNK_SIZE / 2, res, 0, CHUNK_SIZE / 2));
      assertTrue(BufferUtils.equalIncreasingByteArray(CHUNK_SIZE / 2, CHUNK_SIZE / 2, res));
    }
  }

  @Test
  public void positionedReadMulti() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    Random random = new Random();
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      for (int i = 0; i < 10; i++) {
        int pos = random.nextInt(CHUNK_SIZE);
        int len = CHUNK_SIZE - pos;
        byte[] res = new byte[len];
        assertEquals(len,
            inStream.positionedRead(pos, res, 0, len));
        assertTrue(BufferUtils.equalIncreasingByteArray(pos, len, res));
      }
    }
  }

  @Test
  public void seekForward() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    Random random = new Random();
    int pos = 0;
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      for (int i = 0; i < 10; i++) {
        pos += random.nextInt(CHUNK_SIZE - pos);
        inStream.seek(pos);
        assertEquals(pos, inStream.getPos());
        int len = CHUNK_SIZE - pos;
        byte[] res = new byte[len];
        assertEquals(len,
            inStream.read(res, 0, len));
        assertTrue(BufferUtils.equalIncreasingByteArray(pos, len, res));
        if (CHUNK_SIZE == pos) {
          break;
        }
      }
    }
  }

  @Test
  public void seekBackward() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    Random random = new Random();
    int pos = CHUNK_SIZE - 1;
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      for (int i = 0; i < 10; i++) {
        pos -= random.nextInt(pos);
        inStream.seek(pos);
        assertEquals(pos, inStream.getPos());
        int len = CHUNK_SIZE - pos;
        byte[] res = new byte[len];
        assertEquals(len,
            inStream.read(res, 0, len));
        assertTrue(BufferUtils.equalIncreasingByteArray(pos, len, res));
        if (pos <= 0) {
          break;
        }
      }
    }
  }

  @Test
  public void seekToBeginning() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      byte[] res = new byte[CHUNK_SIZE];
      assertEquals(CHUNK_SIZE, inStream.read(res));
      assertTrue(BufferUtils.equalIncreasingByteArray(CHUNK_SIZE, res));
      inStream.seek(0);
      assertEquals(0, inStream.getPos());
      assertEquals(CHUNK_SIZE, inStream.read(res));
      assertTrue(BufferUtils.equalIncreasingByteArray(CHUNK_SIZE, res));
    }
  }

  @Test
  public void seekForwardAndBackward() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    Random random = new Random();
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      for (int i = 0; i < 10; i++) {
        int pos = random.nextInt(CHUNK_SIZE);
        inStream.seek(pos);
        assertEquals(pos, inStream.getPos());
        int len = CHUNK_SIZE - pos;
        byte[] res = new byte[len];
        assertEquals(len,
            inStream.read(res, 0, len));
        assertTrue(BufferUtils.equalIncreasingByteArray(pos, len, res));
      }
    }
  }

  @Test
  public void seekPassEnd() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(IllegalArgumentException.class, () -> inStream.seek(CHUNK_SIZE + 1));
    }
  }

  @Test
  public void seekNegative() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertThrows(IllegalArgumentException.class, () -> inStream.seek(-1));
    }
  }

  @Test
  public void skip() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    Random random = new Random();
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      for (int i = 0; i < 10; i++) {
        inStream.seek(0);
        int skip = random.nextInt(CHUNK_SIZE);
        assertEquals(skip, inStream.skip(skip));
        assertEquals(skip, inStream.getPos());
        int len = CHUNK_SIZE - skip;
        byte[] res = new byte[len];
        assertEquals(len,
            inStream.read(res, 0, len));
        assertTrue(BufferUtils.equalIncreasingByteArray(skip, len, res));
      }
    }
  }

  @Test
  public void skipToEnd() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(CHUNK_SIZE, inStream.skip(CHUNK_SIZE));
      assertEquals(-1, inStream.read());
    }
  }

  @Test
  public void skipPassEnd() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(CHUNK_SIZE, inStream.skip(CHUNK_SIZE + 1));
    }
  }

  @Test
  public void skipNegative() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(0, inStream.skip(-1));
    }
  }

  @Test
  public void getPosition() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(0, inStream.getPos());
      inStream.read();
      assertEquals(1, inStream.getPos());
      int len = CHUNK_SIZE / 2;
      inStream.read(new byte[len], 0, len);
      assertEquals(1 + len, inStream.getPos());
      len = CHUNK_SIZE / 4;
      inStream.read(ByteBuffer.allocate(len), 0, len);
      assertEquals(1 + CHUNK_SIZE / 4 * 3, inStream.getPos());
    }
  }

  @Test
  public void remaining() throws IOException {
    String ufsPath = getUfsPath();
    createFile(ufsPath, CHUNK_SIZE);
    try (UfsFileInStream inStream = getStream(ufsPath)) {
      assertEquals(CHUNK_SIZE, inStream.remaining());
      inStream.read();
      assertEquals(CHUNK_SIZE - 1, inStream.remaining());
      int len = CHUNK_SIZE / 2;
      inStream.read(new byte[len], 0, len);
      assertEquals(CHUNK_SIZE - len - 1, inStream.remaining());
      len = CHUNK_SIZE / 4;
      inStream.read(ByteBuffer.allocate(len), 0, len);
      assertEquals(CHUNK_SIZE / 4 - 1, inStream.remaining());
    }
  }

  private String getUfsPath() {
    return Paths.get(mRootUfs, String.valueOf(UUID.randomUUID())).toString();
  }

  private UfsFileInStream getStream(String ufsPath) throws IOException {
    return new UfsFileInStream(mUfs.open(ufsPath),
        ((UfsFileStatus) mUfs.getStatus(ufsPath)).getContentLength());
  }

  private void createFile(String ufsPath, int len) throws IOException {
    createFile(ufsPath, 0, len);
  }

  private void createFile(String ufsPath, int start, int len) throws IOException {
    try (OutputStream outStream = mUfs.create(ufsPath)) {
      outStream.write(BufferUtils.getIncreasingByteArray(start, len));
    }
  }
}
