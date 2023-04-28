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

package alluxio;

import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A position reader test to test position read from the given reader.
 * Note that the reader should be set up to read file write from (byte) 0 to (byte) (fileLen - 1).
 */
public class PositionReaderTest {
  private final PositionReader mReader;
  private final int mFileLen;
  private final Random mRandom = new Random();

  /**
   * @param reader
   * @param fileLen
   */
  public PositionReaderTest(PositionReader reader, int fileLen) {
    mReader = reader;
    mFileLen = fileLen;
  }

  /**
   * Tests all corner cases.
   */
  public void testAllCornerCases() throws IOException {
    byteArrayInvalidLength();
    byteArrayInvalidLength2();
    byteArrayNegativeLength();
    byteArrayNegativeLength2();
    byteArrayNegativePosition();
    byteArrayNegativePosition2();
    byteArrayZeroLength();
    byteArrayZeroLength2();

    byteBufferInvalidLength();
    byteBufferNegativeLength();
    byteBufferNegativePosition();
    byteBufferZeroLength();

    byteBufInvalidLength();
    byteBufNegativeLength();
    byteBufNegativePosition();
    byteBufZeroLength();
  }

  /**
   * Tests read random parts.
   */
  public void testReadRandomPart() throws IOException {
    byteArrayReadRandomPart();
    byteBufferReadRandomPart();
    byteBufReadRandomPart();
  }

  /**
   * Tests concurrently read from position reader.
   */
  public void concurrentReadPart() throws Exception {
    int threadNum = Math.min(mFileLen, 10);
    if (threadNum <= 1) {
      return;
    }
    ExecutorService executor = Executors.newFixedThreadPool(threadNum);
    try {
      List<Callable<Boolean>> tasks = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        int seed = mRandom.nextInt(3);
        tasks.add(() -> {
          try {
            switch (seed) {
              case 0:
                byteArrayReadRandomPart();
                break;
              case 1:
                byteBufferReadRandomPart();
                break;
              case 2:
                byteBufReadRandomPart();
                break;
              default:
                throw new IOException("should not happen");
            }
            return true;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      }
      List<Future<Boolean>> results = executor.invokeAll(tasks);
      for (Future<Boolean> result : results) {
        if (!result.get()) {
          throw new IOException("Data corrupted");
        }
      }
    } finally {
      executor.shutdown();
    }
  }

  private void byteBufferReadRandomPart() throws IOException {
    int testNum = Math.min(mFileLen, 50);
    ByteBuffer buf = ByteBuffer.allocate(mFileLen);
    for (int i = 0; i < testNum; i++) {
      int position = mRandom.nextInt(mFileLen);
      int length = mRandom.nextInt(mFileLen - position);
      if (length == 0) {
        continue;
      }
      buf.position(0);
      buf.limit(length);
      int totalRead = 0;
      int currentRead;
      while (totalRead < length) {
        currentRead = mReader.read(position + totalRead, buf, length - totalRead);
        if (currentRead <= 0) {
          break;
        }
        totalRead += currentRead;
      }
      if (totalRead != length) {
        throw new IOException(String.format(
            "Failed to read data from position %s of length %s from reader, only read %s",
            position, length, totalRead));
      }
      if (!BufferUtils.equalIncreasingByteBuffer(position, length, buf)) {
        throw new IOException("Data corrupted");
      }
    }
  }

  private void byteBufReadRandomPart() throws IOException {
    int testNum = Math.min(mFileLen, 50);
    ByteBuf buf = Unpooled.buffer(mFileLen);
    try {
      for (int i = 0; i < testNum; i++) {
        int position = mRandom.nextInt(mFileLen);
        int length = mRandom.nextInt(mFileLen - position);
        if (length == 0) {
          continue;
        }
        buf.clear();
        buf.capacity(length);
        int totalRead = 0;
        int currentRead;
        while (totalRead < length) {
          currentRead = mReader.read(position + totalRead, buf, length - totalRead);
          if (currentRead <= 0) {
            break;
          }
          totalRead += currentRead;
        }
        if (totalRead != length) {
          throw new IOException(String.format(
              "Failed to read data from position %s of length %s from reader, only read %s",
              position, length, totalRead));
        }
        if (!BufferUtils.equalIncreasingByteBuf(position, length, buf)) {
          throw new IOException("Data corrupted");
        }
      }
    } finally {
      buf.release();
    }
  }

  private void byteArrayReadRandomPart() throws IOException {
    int testNum = Math.min(mFileLen, 50);
    byte[] buf = new byte[mFileLen];
    for (int i = 0; i < testNum; i++) {
      int position = mRandom.nextInt(mFileLen);
      int length = mRandom.nextInt(mFileLen - position);
      if (length == 0) {
        continue;
      }
      int totalRead = 0;
      int currentRead;
      while (totalRead < length) {
        currentRead = mReader.read(position + totalRead, buf, length - totalRead);
        if (currentRead <= 0) {
          break;
        }
        totalRead += currentRead;
      }
      if (totalRead != length) {
        throw new IOException(String.format(
            "Failed to read data from position %s of length %s from reader, only read %s",
            position, length, totalRead));
      }
      if (!BufferUtils.startsWithIncreasingByteArray(position, length, buf)) {
        throw new IOException("Data corrupted");
      }
    }
  }

  private void byteBufferZeroLength() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(1);
    if (mReader.read(0, buf, 0) != 0) {
      throw new IOException("read length 0 should return 0");
    }
    buf.flip();
    if (buf.remaining() != 0) {
      throw new IOException("read length 0 should not read any data from buf");
    }
  }

  private void byteBufZeroLength() throws IOException {
    ByteBuf buf = Unpooled.buffer(1);
    if (mReader.read(0, buf, 0) != 0) {
      throw new IOException("read length 0 should return 0");
    }
    try {
      buf.readByte();
      throw new IOException("read length 0 should not read any data from buf");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }

  private void byteArrayZeroLength() throws IOException {
    byte[] byteArray = new byte[1];
    if (mReader.read(0, byteArray, 0) != 0) {
      throw new IOException("read length 0 should return 0");
    }
  }

  private void byteArrayZeroLength2() throws IOException {
    byte[] byteArray = new byte[1];
    if (mReader.read(0, byteArray, 0, 0) != 0) {
      throw new IOException("read length 0 should return 0");
    }
  }

  private void byteBufferInvalidLength() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(1);
    try {
      mReader.read(0, buf, 2);
      throw new IOException("Read length should not be bigger than given buffer length");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteBufInvalidLength() throws IOException {
    ByteBuf buf = Unpooled.buffer(1);
    try {
      mReader.read(0, buf, 2);
      throw new IOException("Read length should not be bigger than given buffer length");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteArrayInvalidLength() throws IOException {
    byte[] byteArray = new byte[1];
    try {
      mReader.read(0, byteArray, 2);
      throw new IOException("Read length should not be bigger than given byteArray length");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteArrayInvalidLength2() throws IOException {
    byte[] byteArray = new byte[1];
    try {
      mReader.read(0, byteArray, 0, 2);
      throw new IOException("Read length should not be bigger than given byteArray length");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteBufferNegativeLength() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(1);
    try {
      mReader.read(0, buf, -1);
      throw new IOException("Read should error out when given length is negative");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteBufNegativeLength() throws IOException {
    ByteBuf buf = Unpooled.buffer(1);
    try {
      mReader.read(0, buf, -1);
      throw new IOException("Read should error out when given length is negative");
    } catch (IllegalArgumentException e) {
      // expected
    } finally {
      buf.release();
    }
  }

  private void byteArrayNegativeLength() throws IOException {
    byte[] byteArray = new byte[1];
    try {
      mReader.read(0, byteArray, -1);
      throw new IOException("Read should error out when given length is negative");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteArrayNegativeLength2() throws IOException {
    byte[] byteArray = new byte[1];
    try {
      mReader.read(0, byteArray, 0, -1);
      throw new IOException("Read should error out when given length is negative");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteBufferNegativePosition() throws IOException {
    if (mFileLen < 1) {
      return;
    }
    ByteBuffer buf = ByteBuffer.allocate(1);
    try {
      mReader.read(-1, buf, 1);
      throw new IOException("Read should error out when given position is negative");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteBufNegativePosition() throws IOException {
    if (mFileLen < 1) {
      return;
    }
    ByteBuf buf = Unpooled.buffer(1);
    try {
      mReader.read(-1, buf, 1);
      throw new IOException("Read should error out when given position is negative");
    } catch (IllegalArgumentException e) {
      // expected
    } finally {
      buf.release();
    }
  }

  private void byteArrayNegativePosition() throws IOException {
    if (mFileLen < 1) {
      return;
    }
    byte[] byteArray = new byte[1];
    try {
      mReader.read(-1, byteArray, 1);
      throw new IOException("Read should error out when given position is negative");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private void byteArrayNegativePosition2() throws IOException {
    if (mFileLen < 1) {
      return;
    }
    byte[] byteArray = new byte[1];
    try {
      mReader.read(-1, byteArray, 0, 1);
      throw new IOException("Read should error out when given position is negative");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
