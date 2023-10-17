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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import alluxio.ByteArrayPositionReader;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.collections.Pair;
import alluxio.file.ReadTargetBuffer;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

//@RunWith(Parameterized.class)
public class PositionReadFileInStreamTest {
  //  private final PositionReader mPositionReader;
//  private final byte[] mBuffer;
//  private final int mDataLength;
//  private final int mBufferSize;
  private PositionReadFileInStream fileInStream;

  private PrefetchCache mockPrefetchCache;
  private PositionReader mockPositionReader;
  @Before
  public void setUp(){
    mockPrefetchCache = Mockito.mock(PrefetchCache.class);
    mockPositionReader = Mockito.mock(PositionReader.class);
    fileInStream = new PositionReadFileInStream(mockPositionReader, 100,mockPrefetchCache);
  }
  @After
  public void tearDown() throws IOException {
    fileInStream.close();
  }
  @Test
  public void testReadDataFromCacheAndSource_BytesReadFromCache() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(anyLong(), any(ByteBuffer.class))).thenReturn(5);
    when(mockPositionReader.read(anyLong(), any(ByteBuffer.class), anyInt())).thenReturn(2);
    when(mockPrefetchCache.prefetch(any(PositionReader.class),anyLong(), anyInt())).thenReturn(3);

    int bytesRead = fileInStream.readDataFromCacheAndSource(0, buffer, true);

    assertEquals(12, bytesRead);
//    assertEquals(10, fileInStream.getPos());
  }

  @Test
  public void testReadDataFromCacheAndSource_PrefetchFailed() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(0);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(-1);

    int bytesRead = fileInStream.readDataFromCacheAndSource(0, buffer, true);

    assertEquals(-1, bytesRead);
    assertEquals(0, fileInStream.getPos());
  }
  @Test
  public void testReadDataFromCacheAndSource_ReadFailed() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(0);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(3);
    when(mockPositionReader.read(anyLong(), any(ByteBuffer.class), anyInt())).thenReturn(-1);

    int bytesRead = fileInStream.readDataFromCacheAndSource(0, buffer, true);

    assertEquals(-1, bytesRead);
    assertEquals(0, fileInStream.getPos());
  }
  @Test
  public void testReadDataFromCacheAndSource_NoBytesToRead() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(0);

    int bytesRead = fileInStream.readDataFromCacheAndSource(0, buffer, true);

    assertEquals(0, bytesRead);
    assertEquals(0, fileInStream.getPos());
  }


  @Test
  public void testReadDataFromCacheAndSource_UpdatePosition() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(5);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(3);
    when(mockPositionReader.read(5, buffer, buffer.remaining())).thenReturn(2);

    int bytesRead = fileInStream.readDataFromCacheAndSource(0, buffer, true);

    assertEquals(7, bytesRead);
    assertEquals(7, fileInStream.getPos());
  }

  @Test
  public void testReadDataFromCacheAndSource_DontUpdatePosition() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(5);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(3);
    when(mockPositionReader.read(5, buffer, buffer.remaining())).thenReturn(2);

    int bytesRead = fileInStream.readDataFromCacheAndSource(0, buffer, false);

    assertEquals(7, bytesRead);
    assertEquals(0, fileInStream.getPos());
  }
  @Test
  public void testSeek_PositivePosition() throws IOException {
    fileInStream.seek(50);

    assertEquals(50, fileInStream.getPos());
  }

  @Test
  public void testSeek_NegativePosition() throws IOException {
    try{
      fileInStream.seek(-1);
    } catch (Exception e) {
      assertEquals("Seek position is negative: -1", e.getMessage());
    }
  }

  @Test
  public void testSeek_PositionPastEnd() throws IOException {
    try {
      fileInStream.seek(101);
    } catch (Exception e) {
      assertEquals("Seek position past the end of the read region (block or file).", e.getMessage());
    }
  }

  @Test
  public void testSeek_SamePosition() throws IOException {
    fileInStream.seek(50);
    fileInStream.seek(50);

    assertEquals(50, fileInStream.getPos());
  }

  @Test
  public void testSkip_PositiveBytesToSkip() throws IOException {
    long bytesSkipped = fileInStream.skip(30);

    assertEquals(30, bytesSkipped);
    assertEquals(30, fileInStream.getPos());
  }
  @Test
  public void testSkip_NegativeBytesToSkip() throws IOException {
    long bytesSkipped = fileInStream.skip(-10);

    assertEquals(0, bytesSkipped);
    assertEquals(0, fileInStream.getPos());
  }

  @Test
  public void testSkip_ExceedRemainingBytes() throws IOException {
    long bytesSkipped = fileInStream.skip(150);

    assertEquals(100, bytesSkipped);
    assertEquals(100, fileInStream.getPos());
  }
  @Test
  public void testRead_BytesReadFromCache() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(5);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(5);
    when(mockPositionReader.read(anyLong(), any(ByteBuffer.class), anyInt())).thenReturn(3);

    int bytesRead = fileInStream.read(buffer, 0, 10);

    assertEquals(8, bytesRead);
    assertEquals(8, fileInStream.getPos());
  }
  @Test
  public void testPositionedRead_BytesReadFromCache() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(5);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(5);
    when(mockPositionReader.read(anyLong(), any(ByteBuffer.class), anyInt())).thenReturn(3);

    int bytesRead = fileInStream.positionedRead(0, buffer.array(), 0, 10);

    assertEquals(8, bytesRead);
    assertEquals(0, fileInStream.getPos());
  }

  @Test
  public void testRead_BytesReadFromByteBuffer() throws IOException {
    byte[] byteArray = new byte[10];
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    when(mockPrefetchCache.fillWithCache(0, buffer)).thenReturn(5);
    when(mockPrefetchCache.prefetch(any(PositionReader.class), anyLong(), anyInt())).thenReturn(5);
    when(mockPositionReader.read(anyLong(), any(ByteBuffer.class), anyInt())).thenReturn(3);

    int bytesRead = fileInStream.read(byteArray, 0, 10);

    assertEquals(8, bytesRead);
    assertEquals(8, fileInStream.getPos());
  }
  @Test(expected = NullPointerException.class)
  public void testRead_NullByteArray() throws IOException {
    byte[] byteArray = null;
    fileInStream.read(byteArray, 0, 10);
  }

//  @Parameterized.Parameters(name = "{index}_DL_{0}_BS_{1}")
//  public static Iterable<Object[]> data() {
//    return Arrays.asList(new Object[][] {
//        /* data length,       buffer size */
//        { Constants.KB,       63 },
//        { Constants.KB,       64 },
//        { Constants.KB,       65 },
//        { Constants.KB,       Constants.KB - 1 },
//        { Constants.KB,       Constants.KB },
//        { Constants.MB * 10,  Constants.KB * 10 },
//        { Constants.MB * 10,  Constants.KB * 10 - 1},
//        { Constants.MB * 10,  Constants.KB * 10 + 1},
//    });
//  }

//  public PositionReadFileInStreamTest(int dataLength, int bufferSize) {
//    mDataLength = dataLength;
//    mBufferSize = bufferSize;
//    mPositionReader = new ByteArrayPositionReader(BufferUtils.getIncreasingByteArray(dataLength));
//    mBuffer = new byte[bufferSize];
//  }

//  @Test
//  public void sequentialReadWithIdenticalBufferSize() throws Exception {
//    testSequentialRead(1.0);
//  }
//
//  @Test
//  public void sequentialReadWithRandomBufferSizes() throws Exception {
//    testSequentialRead(0.5);
//  }

//  private void testSequentialRead(double smallBufProb) throws Exception {
//    PositionReader spy = Mockito.spy(mPositionReader);
//    PositionReadFileInStream stream = new PositionReadFileInStream(spy, mDataLength);
//    int readLength;
//    int totalBytesRead = 0;
//    int bytesRead;
//    int numUnbufferedCalls = 0;
//    for (Pair<Integer, Integer> pair :
//        generateReadSequence(1.0, smallBufProb, mDataLength, mBufferSize)) {
//      readLength = pair.getSecond();
//      if (stream.getBufferedPosition() + stream.getBufferedLength()
//          < stream.getPos() + readLength) {
//        numUnbufferedCalls += 1;
//      }
//      bytesRead = stream.read(mBuffer, 0, readLength);
//      assertEquals(bytesRead, readLength);
//      totalBytesRead += bytesRead;
//      assertTrue(BufferUtils.equalIncreasingByteBuffer(
//          totalBytesRead - bytesRead, bytesRead, ByteBuffer.wrap(mBuffer, 0, bytesRead)));
//      verify(spy, times(numUnbufferedCalls)
//          .description("position: " + stream.getPos() + ", length: " + readLength))
//          .readInternal(anyLong(), any(ReadTargetBuffer.class), anyInt());
//    }
//  }

//  @Test
//  public void positionedReadWithRandomBufferSize() throws Exception {
//    PositionReader spy = Mockito.spy(mPositionReader);
//    PositionReadFileInStream stream = new PositionReadFileInStream(spy, mDataLength);
//    int readLength;
//    int bytesRead;
//    int numUnbufferedCalls = 0;
//    int position;
//    for (Pair<Integer, Integer> pair :
//        generateReadSequence(0.7, 0.0, mDataLength, mBufferSize)) {
//      position = pair.getFirst();
//      readLength = pair.getSecond();
//      if (stream.getBufferedPosition() + stream.getBufferedLength() < position + readLength) {
//        numUnbufferedCalls += 1;
//      }
//      bytesRead = stream.positionedRead(position, mBuffer, 0, readLength);
//      assertEquals(bytesRead, readLength);
//      assertTrue(BufferUtils.equalIncreasingByteBuffer(
//          position, bytesRead, ByteBuffer.wrap(mBuffer, 0, bytesRead)));
//      verify(spy, times(numUnbufferedCalls)
//          .description("position: " + position + ", length: " + readLength))
//          .readInternal(anyLong(), any(ReadTargetBuffer.class), anyInt());
//    }
//  }

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
//  private List<Pair<Integer, Integer>> generateReadSequence(
//      double continuousProb, double fixedBufferProb, int fileLength, int bufferSize) {
//    final int randSeed = 0xdeadd00d;
//    Random rng = new Random(randSeed);
//    List<Pair<Integer, Integer>> sequence = new ArrayList<>();
//    int lastPosition = 0;
//    int position;
//    int size;
//    while (lastPosition < fileLength) {
//      if (rng.nextDouble() <= continuousProb) {
//        position = lastPosition;
//      } else {
//        position = lastPosition + rng.nextInt(fileLength - lastPosition + 1);
//      }
//      if (rng.nextDouble() <= fixedBufferProb) {
//        size = bufferSize;
//      } else {
//        size = rng.nextInt(bufferSize + 1);
//      }
//      size = Math.min(size, fileLength - position);
//      lastPosition = position + size;
//      Pair<Integer, Integer> pair = new Pair<>(position, size);
//      sequence.add(pair);
//    }
//    return sequence;
//  }
}