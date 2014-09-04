package tachyon.client;

import java.io.IOException;

/**
 * FileInStream implementation of TachyonFile.
 */
public class FileInStream extends InStream {
  private final long mFileLength;
  private final long mBlockCapacity;

  private long mCurrentPosition;
  private int mCurrentBlockIndex;
  private BlockInStream mCurrentBlockInStream;
  private long mCurrentBlockLeft;

  private boolean mClosed = false;

  private Object mUFSConf = null;

  /**
   * @param file
   *          the file to be read
   * @param opType
   *          the InStream's read type
   * @throws IOException
   */
  public FileInStream(TachyonFile file, ReadType opType) throws IOException {
    this(file, opType, null);
  }

  /**
   * @param file
   *          the file to be read
   * @param opType
   *          the InStream's read type
   * @param ufsConf
   *          the under file system configuration
   * @throws IOException
   */
  public FileInStream(TachyonFile file, ReadType opType, Object ufsConf) throws IOException {
    super(file, opType);

    mFileLength = file.length();
    mBlockCapacity = file.getBlockSizeByte();

    mCurrentPosition = 0;
    mCurrentBlockIndex = -1;
    mCurrentBlockInStream = null;
    mCurrentBlockLeft = 0;

    mUFSConf = ufsConf;
  }

  private void checkAndAdvanceBlockInStream() throws IOException {
    if (mCurrentBlockLeft == 0) {
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }

      mCurrentBlockIndex = getCurrentBlockIndex();
      mCurrentBlockInStream = BlockInStream.get(mFile, mReadType, mCurrentBlockIndex, mUFSConf);
      mCurrentBlockLeft = mBlockCapacity;
    }
  }

  @Override
  public void close() throws IOException {
    if (!mClosed && mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }

    mClosed = true;
  }

  private int getCurrentBlockIndex() {
    return (int) (mCurrentPosition / mBlockCapacity);
  }

  @Override
  public int read() throws IOException {
    if (mCurrentPosition >= mFileLength) {
      return -1;
    }

    checkAndAdvanceBlockInStream();

    mCurrentPosition ++;
    mCurrentBlockLeft --;
    return mCurrentBlockInStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int tOff = off;
    int tLen = len;

    while (tLen > 0 && mCurrentPosition < mFileLength) {
      checkAndAdvanceBlockInStream();

      int tRead = mCurrentBlockInStream.read(b, tOff, tLen);
      if (tRead == -1) {
        // mCurrentBlockInStream has reached its block boundary
        continue;
      }

      mCurrentPosition += tRead;
      mCurrentBlockLeft -= tRead;
      tLen -= tRead;
      tOff += tRead;
    }

    return len - tLen;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mCurrentPosition == pos) {
      return;
    }
    if (pos < 0) {
      throw new IOException("pos is negative: " + pos);
    }
    if (pos > mFileLength) {
      throw new IOException("pos is past EOF: " + pos + ", fileSize = " + mFileLength);
    }

    if ((int) (pos / mBlockCapacity) != mCurrentBlockIndex) {
      mCurrentBlockIndex = (int) (pos / mBlockCapacity);
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }
      mCurrentBlockInStream = BlockInStream.get(mFile, mReadType, mCurrentBlockIndex, mUFSConf);
    }
    mCurrentBlockInStream.seek(pos % mBlockCapacity);
    mCurrentPosition = pos;
    mCurrentBlockLeft = mBlockCapacity - (pos % mBlockCapacity);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long ret = n;
    if (mCurrentPosition + n >= mFile.length()) {
      ret = mFile.length() - mCurrentPosition;
      mCurrentPosition += ret;
    } else {
      mCurrentPosition += n;
    }

    int tBlockIndex = (int) (mCurrentPosition / mBlockCapacity);
    if (tBlockIndex != mCurrentBlockIndex) {
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }

      mCurrentBlockIndex = tBlockIndex;
      mCurrentBlockInStream = BlockInStream.get(mFile, mReadType, mCurrentBlockIndex, mUFSConf);
      long shouldSkip = mCurrentPosition % mBlockCapacity;
      long skip = mCurrentBlockInStream.skip(shouldSkip);
      mCurrentBlockLeft = mBlockCapacity - skip;
      if (skip != shouldSkip) {
        throw new IOException("The underlayer BlockInStream only skip " + skip + " instead of "
            + shouldSkip);
      }
    } else {
      long skip = mCurrentBlockInStream.skip(ret);
      if (skip != ret) {
        throw new IOException("The underlayer BlockInStream only skip " + skip + " instead of "
            + ret);
      }
    }

    return ret;
  }
}