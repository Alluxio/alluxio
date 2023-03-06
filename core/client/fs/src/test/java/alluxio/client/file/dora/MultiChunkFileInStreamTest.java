package alluxio.client.file.dora;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import alluxio.client.block.stream.TestDataReader;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class MultiChunkFileInStreamTest {

  TestDataReader.Factory mFactory;
  MultiChunkFileInStream mStream;
  int mSize;
  int mChunkSize;
  byte[] mData;

  @Before
  public void before() {
    mSize = 100;
    mChunkSize = 10;

    mData = new byte[mSize];
    for (int i = 0; i < mSize; i++) {
      mData[i] = (byte) i;
    }
    mFactory = new TestDataReader.Factory(mChunkSize, mData);
    mStream = new MultiChunkFileInStream(mFactory, mSize);
  }

  @Test
  public void singeByteRead() throws Exception {
    for (int i = 0; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void batchRead() throws Exception {
    int batchSize = 10;
    byte[] batch = new byte[batchSize];
    for (int i = 0; i < mSize; i += batchSize) {
      assertEquals(batchSize, mStream.read(batch));
      assertArrayEquals(Arrays.copyOfRange(mData, i, i + batchSize), batch);
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void doubleBatchRead() throws Exception {
    int batchSize = 20;
    byte[] batch = new byte[batchSize];
    for (int i = 0; i < mSize; i += batchSize) {
      ByteBuffer buf = ByteBuffer.wrap(batch);
      assertEquals(mChunkSize, mStream.read(buf));
      buf.limit(batchSize);
      assertEquals(mChunkSize, mStream.read(buf));
      assertArrayEquals(Arrays.copyOfRange(mData, i, i + batchSize), batch);
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void singleBatchSeekForward() throws Exception {
    // first read a byte
    assertEquals(mData[0], mStream.read());
    // seek to the next chunk
    int pos = mChunkSize + 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void singleBatchSeekForward2() throws Exception {
    // first read more than half of a chunk
    for (int i = 0; i < mChunkSize / 3 * 2; i++) {
      assertEquals(mData[i], mStream.read());
    }
    // seek to the next chunk
    int pos = mChunkSize + 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void seekForwardWithinChunk() throws Exception {
    // first read a byte
    assertEquals(mData[0], mStream.read());
    // seek into the chunk
    int pos = mChunkSize - 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void seekBackWithinChunk() throws Exception {
    // read into the first chunk
    for (int i = 0; i < mChunkSize - 2; i++) {
      assertEquals(mData[i], mStream.read());
    }
    // seek back within chunk
    int pos = 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void doubleBatchSeekForward() throws Exception {
    // first read a byte
    assertEquals(mData[0], mStream.read());
    // seek to the next chunk
    int pos = 2 * mChunkSize + 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void doubleBatchSeekForward2() throws Exception {
    // first read more than half of a chunk
    for (int i = 0; i < mChunkSize / 3 * 2; i++) {
      assertEquals(mData[i], mStream.read());
    }
    // seek to the next chunk
    int pos = 2 * mChunkSize + 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void seekForwardOutOfChunk() throws Exception {
    // first read a byte
    assertEquals(mData[0], mStream.read());
    // seek out of range of the chunks
    int pos = 3 * mChunkSize + 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void seekBackOutOfChunk() throws Exception {
    // read into the third chunk
    for (int i = 0; i < 2 * mChunkSize + 2; i++) {
      assertEquals(mData[i], mStream.read());
    }
    // seek before the chunk
    int pos = mChunkSize - 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void singleBatchSeekForwardFromSecond() throws Exception {
    // read into the second chunk
    for (int i = 0; i < mChunkSize + 2; i++) {
      assertEquals(mData[i], mStream.read());
    }
    // seek to the next chunk
    int pos = 2 * mChunkSize + 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void singleBatchSeekBackward() throws Exception {
    // read into the second chunk
    for (int i = 0; i < mChunkSize + 2; i++) {
      assertEquals(mData[i], mStream.read());
    }
    // seek to the previous chunk
    int pos = mChunkSize - 2;
    mStream.seek(pos);
    for (int i = pos; i < mSize; i++) {
      assertEquals(mData[i], (byte) mStream.read());
    }
    assertEquals(-1, mStream.read());
    mStream.close();
  }

  @Test
  public void smallFileRead() throws Exception {
    int size = 22;
    int chunkSize = 10;

    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) i;
    }
    TestDataReader.Factory factory = new TestDataReader.Factory(chunkSize, data);
    MultiChunkFileInStream stream = new MultiChunkFileInStream(factory, size);
    for (int i = 0; i < size; i++) {
      assertEquals(data[i], stream.read());
    }
    assertEquals(-1, stream.read());
    stream.close();
  }
}
