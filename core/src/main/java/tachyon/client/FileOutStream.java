package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * <code>FileOutStream</code> implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 */
public class FileOutStream extends OutStream {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final long mBlockCapacityByte;

  private BlockOutStream mCurrentBlockOutStream;
  private long mCurrentBlockId;
  private long mCurrentBlockLeftByte;
  private List<BlockOutStream> mPreviousBlockOutStreams;
  private long mCachedBytes;

  private OutputStream mCheckpointOutputStream = null;
  private String mUnderFsFile = null;

  private boolean mClosed = false;
  private boolean mCancel = false;

  /**
   * @param file
   *          the output file
   * @param opType
   *          the OutStream's write type
   * @param ufsConf
   *          the under file system configuration
   * @throws IOException
   */
  FileOutStream(TachyonFile file, WriteType opType, Object ufsConf) throws IOException {
    super(file, opType);

    mBlockCapacityByte = file.getBlockSizeByte();

    // TODO Support and test append.
    mCurrentBlockOutStream = null;
    mCurrentBlockId = -1;
    mCurrentBlockLeftByte = 0;
    mPreviousBlockOutStreams = new ArrayList<BlockOutStream>();
    mCachedBytes = 0;

    if (mWriteType.isThrough()) {
      mUnderFsFile = CommonUtils.concat(mTachyonFS.createAndGetUserUfsTempFolder(), mFile.mFileId);
      UnderFileSystem underfsClient = UnderFileSystem.get(mUnderFsFile, ufsConf);
      if (mBlockCapacityByte > Integer.MAX_VALUE) {
        throw new IOException("BLOCK_CAPCAITY (" + mBlockCapacityByte + ") can not bigger than "
            + Integer.MAX_VALUE);
      }
      mCheckpointOutputStream = underfsClient.create(mUnderFsFile, (int) mBlockCapacityByte);
    }
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mCurrentBlockOutStream != null) {
        mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
      }

      Boolean canComplete = false;
      if (mWriteType.isThrough()) {
        if (mCancel) {
          mCheckpointOutputStream.close();
          UnderFileSystem underFsClient = UnderFileSystem.get(mUnderFsFile);
          underFsClient.delete(mUnderFsFile, false);
        } else {
          mCheckpointOutputStream.flush();
          mCheckpointOutputStream.close();
          mTachyonFS.addCheckpoint(mFile.mFileId);
          canComplete = true;
        }
      }

      if (mWriteType.isCache()) {
        try {
          if (mCancel) {
            for (BlockOutStream bos : mPreviousBlockOutStreams) {
              bos.cancel();
            }
          } else {
            for (BlockOutStream bos : mPreviousBlockOutStreams) {
              bos.close();
            }
            canComplete = true;
          }
        } catch (IOException ioe) {
          if (mWriteType.isMustCache()) {
            LOG.error(ioe);
            throw new IOException("Fail to cache: " + mWriteType, ioe);
          } else {
            LOG.warn("Fail to cache for: ", ioe);
          }
        }
      }

      if (canComplete) {
        if (mWriteType.isAsync()) {
          mTachyonFS.asyncCheckpoint(mFile.mFileId);
        }
        mTachyonFS.completeFile(mFile.mFileId);
      }
    }

    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    // TODO We only flush the checkpoint output stream. Flush for RAMFS block streams.
    if (mWriteType.isThrough()) {
      mCheckpointOutputStream.flush();
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockId != -1) {
      if (mCurrentBlockLeftByte != 0) {
        throw new IOException("The current block still has space left, no need to get new block");
      }
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mWriteType.isCache()) {
      mCurrentBlockId = mTachyonFS.getBlockIdBasedOnOffset(mFile.mFileId, mCachedBytes);
      mCurrentBlockLeftByte = mBlockCapacityByte;

      mCurrentBlockOutStream =
          new BlockOutStream(mFile, mWriteType, (int) (mCachedBytes / mBlockCapacityByte));
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }

    if (mWriteType.isCache()) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockLeftByte == 0) {
            getNextBlock();
          } else if (mCurrentBlockLeftByte < 0 || mCurrentBlockOutStream == null) {
            throw new IOException("mCurrentBlockLeftByte " + mCurrentBlockLeftByte + " "
                + mCurrentBlockOutStream);
          }
          if (mCurrentBlockLeftByte >= tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCurrentBlockLeftByte -= tLen;
            mCachedBytes += tLen;
            tOff += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) mCurrentBlockLeftByte);
            tOff += mCurrentBlockLeftByte;
            tLen -= mCurrentBlockLeftByte;
            mCachedBytes += mCurrentBlockLeftByte;
            mCurrentBlockLeftByte = 0;
          }
        }
      } catch (IOException e) {
        if (mWriteType.isMustCache()) {
          LOG.error(e.getMessage(), e);
          throw new IOException("Fail to cache: " + mWriteType, e);
        } else {
          LOG.warn("Fail to cache for: ", e);
        }
      }
    }

    if (mWriteType.isThrough()) {
      mCheckpointOutputStream.write(b, off, len);
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (mWriteType.isCache()) {
      try {
        if (mCurrentBlockId == -1 || mCurrentBlockLeftByte == 0) {
          getNextBlock();
        }
        // TODO Cache the exception here.
        mCurrentBlockOutStream.write(b);
        mCurrentBlockLeftByte --;
        mCachedBytes ++;
      } catch (IOException e) {
        if (mWriteType.isMustCache()) {
          LOG.error(e.getMessage(), e);
          throw new IOException("Fail to cache: " + mWriteType, e);
        } else {
          LOG.warn("Fail to cache for: ", e);
        }
      }
    }

    if (mWriteType.isThrough()) {
      mCheckpointOutputStream.write(b);
    }
  }
}
