package alluxio.underfs;

import alluxio.Seekable;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * A read only {@link SeekableByteChannel} for Alluxio UFS.
 */
public class SeekableReadChannel implements Seekable, ReadableByteChannel {
  private boolean mIsOpen = true;
  private final SeekableUnderFileInputStream mInputStream;

  /**
   * Constructs a new {@link SeekableReadChannel}.
   *
   * @param inputStream the input stream to read from
   */
  public SeekableReadChannel(SeekableUnderFileInputStream inputStream) {
    mInputStream = inputStream;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throwIfNotOpen();
    int maxReadable = dst.remaining();
    int position = dst.position();
    synchronized (this) {
      int bytesRead = mInputStream.read(dst.array(), position, maxReadable);
      Preconditions.checkState(bytesRead <= maxReadable, "buffer overflow");
      if (bytesRead > 0) {
        dst.position(position + bytesRead);
      }
      return bytesRead;
    }
  }

  @Override
  public boolean isOpen() {
    return mIsOpen;
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
    mIsOpen = false;
  }

  @Override
  public long getPos() throws IOException {
    throwIfNotOpen();
    return mInputStream.getPos();
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IllegalArgumentException("Seek position is negative: " + pos);
    }
    throwIfNotOpen();
    mInputStream.seek(pos);
  }

  /**
   * Throws if this channel is not currently open.
   */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }
}
