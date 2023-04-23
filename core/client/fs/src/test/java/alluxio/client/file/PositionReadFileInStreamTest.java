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

package alluxio.client.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.ByteArrayPositionReader;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.collections.Pair;
import alluxio.file.ReadTargetBuffer;
import alluxio.util.io.BufferUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public class PositionReadFileInStreamTest {
  private final PositionReader mPositionReader;
  private final byte[] mBuffer;
  private final int mDataLength;
  private final int mBufferSize;

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

  public PositionReadFileInStreamTest(int dataLength, int bufferSize) {
    mDataLength = dataLength;
    mBufferSize = bufferSize;
    mPositionReader = new ByteArrayPositionReader(BufferUtils.getIncreasingByteArray(dataLength));
    mBuffer = new byte[bufferSize];
  }

  @Test
  public void sequentialReadWithIdenticalBufferSize() throws Exception {
    testSequentialRead(1.0);
  }

  @Test
  public void sequentialReadWithRandomBufferSizes() throws Exception {
    testSequentialRead(0.5);
  }

  private void testSequentialRead(double smallBufProb) throws Exception {
    PositionReader spy = Mockito.spy(mPositionReader);
    PositionReadFileInStream stream = new PositionReadFileInStream(spy, mDataLength);
    int readLength;
    int totalBytesRead = 0;
    int bytesRead;
    int numUnbufferedCalls = 0;
    for (Pair<Integer, Integer> pair :
        generateReadSequence(1.0, smallBufProb, mDataLength, mBufferSize)) {
      readLength = pair.getSecond();
      if (stream.getBufferedPosition() + stream.getBufferedLength()
          < stream.getPos() + readLength) {
        numUnbufferedCalls += 1;
      }
      bytesRead = stream.read(mBuffer, 0, readLength);
      assertEquals(bytesRead, readLength);
      totalBytesRead += bytesRead;
      assertTrue(BufferUtils.equalIncreasingByteBuffer(
          totalBytesRead - bytesRead, bytesRead, ByteBuffer.wrap(mBuffer, 0, bytesRead)));
      verify(spy, times(numUnbufferedCalls)
          .description("position: " + stream.getPos() + ", length: " + readLength))
          .readInternal(anyLong(), any(ReadTargetBuffer.class), anyInt());
    }
  }

  @Test
  public void positionedReadWithRandomBufferSize() throws Exception {
    PositionReader spy = Mockito.spy(mPositionReader);
    PositionReadFileInStream stream = new PositionReadFileInStream(spy, mDataLength);
    int readLength;
    int bytesRead;
    int numUnbufferedCalls = 0;
    int position;
    for (Pair<Integer, Integer> pair :
        generateReadSequence(0.7, 0.0, mDataLength, mBufferSize)) {
      position = pair.getFirst();
      readLength = pair.getSecond();
      if (stream.getBufferedPosition() + stream.getBufferedLength() < position + readLength) {
        numUnbufferedCalls += 1;
      }
      bytesRead = stream.positionedRead(position, mBuffer, 0, readLength);
      assertEquals(bytesRead, readLength);
      assertTrue(BufferUtils.equalIncreasingByteBuffer(
          position, bytesRead, ByteBuffer.wrap(mBuffer, 0, bytesRead)));
      verify(spy, times(numUnbufferedCalls)
          .description("position: " + position + ", length: " + readLength))
          .readInternal(anyLong(), any(ReadTargetBuffer.class), anyInt());
    }
  }

  /**
   * Generates a sequence of read positions and length.
   *
   * @param continuousProb probability of each read operation starts from the end position of
   *                       the last operation. If 1.0, then the whole read will be sequential
   * @param fixedBufferProb probability that the read size is equal to the buffer size. If 1.0,
   *                        then every read is made with the same buffer size
   * @param fileLength the total length of the file being read
   * @param bufferSize the size of the buffer for one read
   * @return a sequence of pairs of (position, length)
   */
  private List<Pair<Integer, Integer>> generateReadSequence(
      double continuousProb, double fixedBufferProb, int fileLength, int bufferSize) {
    final int randSeed = 0xdeadd00d;
    Random rng = new Random(randSeed);
    List<Pair<Integer, Integer>> sequence = new ArrayList<>();
    int lastPosition = 0;
    int position;
    int size;
    while (lastPosition < fileLength) {
      if (rng.nextDouble() <= continuousProb) {
        position = lastPosition;
      } else {
        position = lastPosition + rng.nextInt(fileLength - lastPosition + 1);
      }
      if (rng.nextDouble() <= fixedBufferProb) {
        size = bufferSize;
      } else {
        size = rng.nextInt(bufferSize + 1);
      }
      size = Math.min(size, fileLength - position);
      lastPosition = position + size;
      Pair<Integer, Integer> pair = new Pair<>(position, size);
      sequence.add(pair);
    }
    return sequence;
  }
}
