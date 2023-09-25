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

package alluxio.client.file.dora;

import static org.junit.Assert.assertTrue;

import alluxio.ByteArrayPositionReader;
import alluxio.CloseableSupplier;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.client.file.dora.netty.PartialReadException;
import alluxio.file.ByteArrayTargetBuffer;
import alluxio.file.ReadTargetBuffer;
import alluxio.util.io.BufferUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Tests for {@link DoraCachePositionReader}.
 */
@RunWith(Parameterized.class)
public class DoraCachePositionReaderTest {
  private final int mLength;
  private final int mBufferSize;
  private final byte[] mData;
  private final PositionReader mReader;
  private final Random mRandom;

  @Parameterized.Parameters(name = "{index}_DL_{0}_BS_{1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        /* data length,       buffer size */
        { Constants.KB,       63 },
        { Constants.KB,       64 },
        { Constants.KB,       65 },
        { Constants.KB,       Constants.KB - 1 },
        { Constants.KB,       Constants.KB },
        { Constants.MB * 10,  Constants.KB * 10 },
        { Constants.MB * 10,  Constants.KB * 10 - 1},
        { Constants.MB * 10,  Constants.KB * 10 + 1},
    });
  }

  public DoraCachePositionReaderTest(int dataLength, int bufferSize) {
    mLength = dataLength;
    mBufferSize = bufferSize;
    mData = BufferUtils.getIncreasingByteArray(dataLength);
    mReader = new DoraCachePositionReader(new FaultyPositionReader(mData), dataLength,
        Optional.of(new CloseableSupplier<>(() -> new ByteArrayPositionReader(mData))));
    mRandom = new Random();
  }

  @Test
  public void positionRead() throws IOException {
    byte[] buffer = new byte[mBufferSize];
    for (int i = 0; i < 100; i++) {
      int start = mRandom.nextInt(mLength);
      int toRead = Math.min(mRandom.nextInt(mLength - start), mBufferSize);
      int read = mReader.read(start, new ByteArrayTargetBuffer(buffer, 0), toRead);
      assertTrue(BufferUtils.equalIncreasingByteBuffer(
          start, read, ByteBuffer.wrap(buffer, 0, read)));
    }
  }

  @Test
  public void sequentialRead() throws IOException {
    byte[] buffer = new byte[mBufferSize];
    for (int i = 0; i < 10; i++) {
      int total = 0;
      while (total < mLength) {
        int toRead = Math.min(mBufferSize, mLength - total);
        int read = mReader.read(total, new ByteArrayTargetBuffer(buffer, 0), toRead);
        assertTrue(BufferUtils.equalIncreasingByteBuffer(
            total, read, ByteBuffer.wrap(buffer, 0, read)));
        total += read;
      }
    }
  }

  /**
   * A PageStore where put can throw IOException on put or delete.
   */
  @NotThreadSafe
  private class FaultyPositionReader extends ByteArrayPositionReader {
    public FaultyPositionReader(byte[] data) {
      super(data);
    }

    private AtomicInteger mCounter = new AtomicInteger(0);
    private Random mRandom = new Random();

    @Override
    public int readInternal(long position, ReadTargetBuffer buffer, int length) throws IOException {
      switch (mCounter.getAndIncrement() % 5) {
        case 0:
          // write fake data but return read 0
          int fakeRead = mRandom.nextInt(length + 1);
          writeFakeData(buffer, fakeRead);
          throw new PartialReadException(length, 0, PartialReadException.CauseType.INTERRUPT,
              new InterruptedException("whatever reason"));
        case 1:
        case 2:
          // Read partial data and throw exception
          int originalOffset = buffer.offset();
          int actualRead = super.readInternal(position, buffer, mRandom.nextInt(length));
          if (actualRead < 0) {
            return -1;
          }
          int returnedRead = mRandom.nextInt(actualRead + 1);
          if (returnedRead < actualRead) {
            buffer.offset(originalOffset + returnedRead);
            writeFakeData(buffer, actualRead - returnedRead);
          }
          throw new PartialReadException(length, returnedRead,
              PartialReadException.CauseType.INTERRUPT,
              new InterruptedException("whatever reason"));
        case 3:
          writeFakeData(buffer, mRandom.nextInt(length));
          throw new IllegalArgumentException("Failed to read for whatever reason");
        default:
          return super.readInternal(position, buffer, length);
      }
    }

    private void writeFakeData(ReadTargetBuffer buffer, int length) {
      byte[] toFill = new byte[length];
      Arrays.fill(toFill, (byte) (-1));
      buffer.writeBytes(toFill, 0, length);
    }
  }
}
