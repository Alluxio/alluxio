package alluxio.client.file.dora;

import alluxio.client.block.stream.DataReader;
import alluxio.client.file.FileInStream;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.exception.status.OutOfRangeException;
import alluxio.network.protocol.databuffer.DataBuffer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * MultiChunkFileInStream gives a FileInStream over chunks.
 * It keeps two sequential chunks within the file, in the assumption
 * that reads will mostly be sequential, but may seek forward and back
 * in regions close to each other. This means that when we move past the
 * current chunk, we will keep the previous chunk in the assumption
 * that we might seek back into it.
 */
public class MultiChunkFileInStream extends FileInStream {
  private static final Logger LOG = LoggerFactory.getLogger(MultiChunkFileInStream.class);

  private final DataReader.Factory mReaderFactory;
  private final long mLength;

  private long mPos = 0;
  private boolean mClosed;
  private DataReader mDataReader;
  private final DataBuffer[] mChunks = new DataBuffer[2];
  private DataBuffer mChunk = null;
  private final int[] mChunkSizes = new int[2];
  private final boolean mDebug;
  private long mChunkStart = 0;

  /**
   * Constructor.
   * @param readerFactory
   * @param length
   */
  public MultiChunkFileInStream(DataReader.Factory readerFactory,
                               long length) {
    mReaderFactory = readerFactory;
    mLength = length;
    mDebug = Configuration.getBoolean(PropertyKey.FUSE_LU_ENABLED);
  }

  @Override
  public long remaining() {
    return mLength - mPos;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b), off, len);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= byteBuffer.capacity(),
        PreconditionMessage.ERR_BUFFER_STATE.toString(), byteBuffer.capacity(), off, len);
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (len == 0) {
      return 0;
    }
    if (mPos == mLength) {
      return -1;
    }
    readChunk();
    if (mChunk == null) {
      closeDataReader();
      if (mPos < mLength) {
        throw new OutOfRangeException(String.format("Block is expected to be %s bytes, "
                + "but only %s bytes are available in the UFS. "
                + "Please retry the read and on the next access, "
                + "Alluxio will sync with the UFS and fetch the updated file content.",
            mLength, mPos));
      }
      return -1;
    }
    int toRead = Math.min(len, mChunk.readableBytes());
    byteBuffer.position(off).limit(off + toRead);
    mChunk.readBytes(byteBuffer);
    mPos += toRead;
    if (mPos == mLength) {
      // a performance improvement introduced by https://github.com/Alluxio/alluxio/issues/14020
      closeDataReader();
    }
    return toRead;
  }

  private void readChunk() throws IOException {
    if (mDataReader == null) {
      mDataReader = mReaderFactory.create(mPos, mLength - mPos);
    }
    if (mChunk == null) { // initial case
      Preconditions.checkState(mChunks[0] == null && mChunks[1] == null);
      mChunkStart = mPos;
      // read the chunks
      for (int i = 0; i < 2; i++) {
        mChunks[i] = mDataReader.readChunk();
        if (mChunks[i] != null) {
          mChunkSizes[i] = mChunks[i].readableBytes();
        }
      }
      mChunk = mChunks[0];
    } else if (mChunk == mChunks[0]) { // we are at the first chunk
      if (mPos == mChunkStart + mChunkSizes[0]) {
        // we are at the end of the first chunk
        // reset to the beginning of the first chunk
        mChunk.skipBytes(-mChunkSizes[0]);
        // update the current chunk
        mChunk = mChunks[1];
      }
    } else if (mChunks[1] != null) { // we are at the second chunk
      Preconditions.checkState(mChunk == mChunks[1]);
      if (mPos == mChunkStart + mChunkSizes[0] + mChunkSizes[1]) {
        // we are at the end of the second chunk
        // reset the position of the chunk and make it the first chunk
        mChunk.skipBytes(-mChunkSizes[1]);
        mChunkStart += mChunkSizes[0];
        mChunks[0].release();
        mChunks[0] = mChunks[1];
        mChunkSizes[0] = mChunkSizes[1];
        // load a new second chunk
        mChunk = mDataReader.readChunk();
        mChunks[1] = mChunk;
        if (mChunk != null) {
          mChunkSizes[1] = mChunks[1].readableBytes();
        } else {
          mChunkSizes[1] = 0; // TODO(tcrain) this line is not needed?
        }
      }
    }
  }

  private void closeDataReader() throws IOException {
    for (int i = 0; i < 2; i++) {
      mChunkSizes[i] = 0;
      if (mChunks[i] != null) {
        mChunks[i].release();
        mChunks[i] = null;
      }
    }
    if (mDataReader != null) {
      mDataReader.close();
    }
    mChunk = null;
    mDataReader = null;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    throw new UnimplementedRuntimeException("unsupported");
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        "Seek position past the end of the read region (block or file).");
    if (pos == mPos) {
      return;
    }
    if (mChunk == null) {
      // we have no chunks loaded so nothing to do
    } else if (pos < mChunkStart) {
      // we are out of the range of the chunks
      closeDataReader();
    } else if (pos < mChunkStart + mChunkSizes[0]) {
      // the seek is in the first chunk
      if (mChunk == mChunks[1]) {
        // if we are in the second chunk reset it
        mChunk.skipBytes((int) ((mChunkStart + mChunkSizes[0]) - mPos));
        // seek within the first chunk
        mChunk = mChunks[0];
        mChunk.skipBytes((int) (pos - mChunkStart));
      } else { // we are in the first chunk, seek within it
        mChunk.skipBytes((int) (pos - mPos));
      }
    } else if (pos < mChunkStart + mChunkSizes[0] + mChunkSizes[1]) {
      // the seek is in the second chunk
      if (mChunk == mChunks[1]) { // seek within the second chunk
        mChunk.skipBytes((int) (pos - mPos));
      } else {
        // if we are in the first chunk, reset it
        mChunk.skipBytes((int) (mChunkStart - mPos));
        // seek within the second chunk
        mChunk = mChunks[1];
        mChunk.skipBytes((int) (pos - (mChunkStart + mChunkSizes[0])));
      }
    } else if (pos < mChunkStart + mChunkSizes[0] + mChunkSizes[1] + mChunkSizes[1]) {
      // the seek is within the range of a new chunk, do the following:
      // - move to the end of the second chunk
      // - read a new chunk and seek within that
      if (mChunk == mChunks[0]) {
        // we are in the first chunk, reset it
        mChunk.skipBytes((int) (mChunkStart - mPos));
        // go to the end of the second chunk
        mChunk = mChunks[1];
        mChunk.skipBytes(mChunkSizes[1]);
      } else {
        // we are in the second chunk, go to the end
        mChunk.skipBytes((int) (mChunkSizes[1] - (mPos - (mChunkStart + mChunkSizes[0]))));
      }
      mPos = mChunkStart + mChunkSizes[0] + mChunkSizes[1];
      readChunk();
      seek(pos);
      return;
    } else {
      // the seek is after the end of the chunks
      closeDataReader();
    }
    mPos = pos;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      closeDataReader();
    } finally {
      mReaderFactory.close();
    }
    mClosed = true;
  }
}
