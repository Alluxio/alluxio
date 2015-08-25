package tachyon.client.next.file;

import tachyon.client.next.CacheType;
import tachyon.client.next.ClientContext;
import tachyon.client.next.ClientOptions;
import tachyon.client.next.OutStream;
import tachyon.client.next.UnderStorageType;
import tachyon.client.next.block.BlockOutStream;
import tachyon.master.MasterClient;
import tachyon.underfs.UnderFileSystem;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Tachyon space in the local machine or remote machines. If the
 * {@link tachyon.client.next.UnderStorageType} is PERSIST, another stream will write the data to
 * the under storage system.
 */
public class ClientFileOutStream extends OutStream {
  private final int mFileId;
  private final ClientOptions mOptions;
  private final long mBlockSize;
  private final CacheType mCacheType;
  private final UnderStorageType mUnderStorageType;
  private final FSContext mContext;
  private final OutputStream mUnderStorageOutputStream;
  private final String mUnderStorageFile;

  private boolean mCanceled;
  private boolean mClosed;
  private long mCachedBytes;
  private BlockOutStream mCurrentBlockOutStream;
  private List<BlockOutStream> mPreviousBlockOutStreams;

  public ClientFileOutStream(int fileId, ClientOptions options) {
    mFileId = fileId;
    mOptions = options;
    mBlockSize = options.getBlockSize();
    mCacheType = options.getCacheType();
    mUnderStorageType = options.getUnderStorageType();
    mContext = FSContext.INSTANCE;
    mPreviousBlockOutStreams = new LinkedList<BlockOutStream>();
    // TODO: Get the Under Storage File correctly
    mUnderStorageFile = null;
    // TODO: Create the under storage output stream
    mUnderStorageOutputStream = mUnderStorageType.shouldPersist() ? null : null;
    mClosed = false;
    mCanceled = false;
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCurrentBlockOutStream != null) {
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    Boolean canComplete = false;
    if (mUnderStorageType.shouldPersist()) {
      if (mCanceled) {
        // TODO: Handle this special case in under storage integrations
        mUnderStorageOutputStream.close();
        UnderFileSystem underFsClient =
            UnderFileSystem.get(mUnderStorageFile, ClientContext.getConf());
        underFsClient.delete(mUnderStorageFile, false);
      } else {
        mUnderStorageOutputStream.flush();
        mUnderStorageOutputStream.close();
        // TODO: Investigate if this RPC can be moved to master
        // mTachyonFS.addCheckpoint(mFile.mFileId);
        canComplete = true;
      }
    }

    if (mCacheType.shouldCache()) {
      try {
        if (mCanceled) {
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
        handleCacheWriteException(ioe);
      }
    }

    if (canComplete) {
      MasterClient masterClient = mContext.acquireMasterClient();
      try {
        masterClient.user_completeFile(mFileId);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    }
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    // TODO: Handle flush for Tachyon storage stream as well
    if (mUnderStorageType.shouldPersist()) {
      mUnderStorageOutputStream.flush();
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (mCacheType.shouldCache()) {
      try {
        if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
          getNextBlock();
        }
        mCurrentBlockOutStream.write(b);
        mCachedBytes ++;
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (mUnderStorageType.shouldPersist()) {
      mUnderStorageOutputStream.write(b);
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

    if (mCacheType.shouldCache()) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
            getNextBlock();
          }
          long currentBlockLeftBytes = mCurrentBlockOutStream.remaining();
          if (currentBlockLeftBytes >= tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCachedBytes += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
            mCachedBytes += currentBlockLeftBytes;
          }
        }
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (mUnderStorageType.shouldPersist()) {
      mUnderStorageOutputStream.write(b, off, len);
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      if (mCurrentBlockOutStream.remaining() > 0) {
        // TODO: Handle this error in a precondition
        throw new IOException("The current block still has space left, no need to get new block");
      }
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mCacheType.shouldCache()) {
      int index = (int) (mCachedBytes / mBlockSize);
      mCurrentBlockOutStream = mContext.getTachyonBS().getOutStream(getBlockId(index), mOptions);
    }
  }

  private long getBlockId(int index) throws IOException {
    MasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.user_getBlockId(mFileId, index);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  private void handleCacheWriteException(IOException ioe) throws IOException {
    if (!mUnderStorageType.shouldPersist()) {
      // TODO: Handle this exception better
      throw new IOException("Fail to cache: " + ioe.getMessage(), ioe);
    } else {
      // TODO: Handle this error
    }
  }
}
