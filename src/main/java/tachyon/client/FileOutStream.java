package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;

/**
 * <code>FileOutStream</code> implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 */
public class FileOutStream extends OutStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final TachyonFS TFS;
  private final int FID;
  private final WriteType WRITE_TYPE;
  private final long BLOCK_CAPACITY;

  private BlockOutStream mCurrentBlockOutStream;
  private boolean mCanCache;
  private long mCurrentBlockId;
  private long mCurrentBlockLeftByte;
  private List<BlockOutStream> mPreviousBlockOutStreams;
  private long mWrittenBytes;

  private OutputStream mCheckpointOutputStream = null;
  private String mUnderFsFile = null;

  private boolean mClosed = false;
  private boolean mCancel = false;

  FileOutStream(TachyonFile file, WriteType opType) throws IOException {
    TFS = file.TFS;
    FID = file.FID;
    WRITE_TYPE = opType;
    BLOCK_CAPACITY = file.getBlockSizeByte();

    mCurrentBlockOutStream = null;
    mCanCache = true;
    mCurrentBlockId = -1;
    mCurrentBlockLeftByte = 0;
    mPreviousBlockOutStreams = new ArrayList<BlockOutStream>();
    mWrittenBytes = 0;

    if (WRITE_TYPE.isThrough()) {
      mUnderFsFile = TFS.createAndGetUserUnderfsTempFolder() + "/" + FID;
      UnderFileSystem underfsClient = UnderFileSystem.get(mUnderFsFile);
      mCheckpointOutputStream = underfsClient.create(mUnderFsFile);
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockId != -1) {
      if (mCurrentBlockLeftByte != 0) {
        throw new IOException("The current block still has space left, no need to get new block");
      }
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (WRITE_TYPE.isCache()) {
      mCurrentBlockId = TFS.getBlockIdBasedOnOffset(FID, mWrittenBytes);
      mCurrentBlockLeftByte = BLOCK_CAPACITY;

      mCurrentBlockOutStream = new BlockOutStream(TFS, FID, WRITE_TYPE, mCurrentBlockId,
          mWrittenBytes, BLOCK_CAPACITY, false);
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (WRITE_TYPE.isCache() && mCanCache) {
      if (mCurrentBlockId == -1 || mCurrentBlockLeftByte == 0) {
        getNextBlock();
      }
      // TODO Cache the exception here.
      mCurrentBlockOutStream.write(b);
      mCurrentBlockLeftByte --;
    }

    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(b);
    }

    mWrittenBytes ++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }

    if (WRITE_TYPE.isCache()) {
      if (mCanCache) {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockLeftByte == 0) {
            getNextBlock();
          }
          if (mCurrentBlockLeftByte > tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCurrentBlockLeftByte -= tLen;
            tOff += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) mCurrentBlockLeftByte);
            tOff += mCurrentBlockLeftByte;
            tLen -= mCurrentBlockLeftByte;
            mCurrentBlockLeftByte = 0;
          }
        }
      } else if (WRITE_TYPE.isMustCache()) {
        throw new IOException("Can not cache: " + WRITE_TYPE);
      }
    }

    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(b, off, len);
    }
  }

  public void write(ByteBuffer buf) throws IOException {
    write(buf.array(), buf.position(), buf.limit() - buf.position());
  }

  public void write(ArrayList<ByteBuffer> bufs) throws IOException {
    for (int k = 0; k < bufs.size(); k ++) {
      write(bufs.get(k));
    }
  }

  /**
   * Not implemented yet.
   */
  @Override
  public void flush() throws IOException {
    throw new IOException("Not supported yet.");
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mCurrentBlockOutStream != null) {
        mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
      }

      if (mCancel) {
        if (WRITE_TYPE.isCache()) {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        }

        if (WRITE_TYPE.isThrough()) {
          mCheckpointOutputStream.close();
          UnderFileSystem underFsClient = UnderFileSystem.get(mUnderFsFile);
          underFsClient.delete(mUnderFsFile, false);
        }
      } else {
        if (WRITE_TYPE.isCache()) {
          try {
            for (int k = 0; k < mPreviousBlockOutStreams.size(); k ++) {
              mPreviousBlockOutStreams.get(k).close();
            }

            TFS.completeFile(FID);
          } catch (IOException e) {
            if (WRITE_TYPE == WriteType.CACHE) {
              throw e;
            }
          }
        }

        if (WRITE_TYPE.isThrough()) {
          mCheckpointOutputStream.flush();
          mCheckpointOutputStream.close();
          TFS.addCheckpoint(FID);
          TFS.completeFile(FID);
        }
      }
    }
    mClosed = true;
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
  }
}
