package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;

/**
 * <code>FileOutStream</code> implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 */
public class FileOutStream extends OutStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final UserConf USER_CONF = UserConf.get();

  private final TachyonFS TFS;
  private final int FID;
  private final WriteType WRITE_TYPE;

  private long mCurrentBlockId;
  private long mCurrentBlockWrittenByte;
  private long mCurrentBlockLeftByte;
  private List<Long> mPreviousBlockIds;
  private long mWrittenBytes;

  private RandomAccessFile mLocalFile;
  private FileChannel mLocalFileChannel;

  private OutputStream mCheckpointOutputStream;

  private ByteBuffer mBuffer;

  private boolean mClosed = false;
  private boolean mCancel = false;

  FileOutStream(TachyonFile file, WriteType opType) throws IOException {
    TFS = file.TFS;
    FID = file.FID;
    WRITE_TYPE = opType;

    mCurrentBlockId = -1;
    mCurrentBlockWrittenByte = 0;
    mCurrentBlockLeftByte = 0;
    mPreviousBlockIds = new ArrayList<Long>();
    mWrittenBytes = 0;

    if (WRITE_TYPE.isThrough()) {
      String underfsFolder = TFS.createAndGetUserUnderfsTempFolder();
      UnderFileSystem underfsClient = UnderFileSystem.get(underfsFolder);
      mCheckpointOutputStream = underfsClient.create(underfsFolder + "/" + FID);
    }

    mBuffer = ByteBuffer.allocate(USER_CONF.FILE_BUFFER_BYTES + 4);
    mBuffer.order(ByteOrder.nativeOrder());
  }

  private synchronized void appendCurrentBuffer(byte[] buf, int offset, 
      int length) throws IOException {
    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(buf, 0, length);
    }
    mWrittenBytes += length;

    if (WRITE_TYPE.isCache()) {
      if (!TFS.requestSpace(length)) {
        if (TFS.isNeedPin(FID)) {
          TFS.outOfMemoryForPinFile(FID);
          throw new IOException("Local tachyon worker does not have enough " +
              "space or no worker for " + FID);
        }

        // TODO this should be okay if it is not must cache.
        throw new IOException("Local tachyon worker does not have enough " +
            "space (" + length + ") or no worker for " + FID);
      }

      if (mCurrentBlockId == -1) {
        getNextBlock();
      }

      int addByte = length;
      if (addByte > mCurrentBlockLeftByte) {
        addByte = (int) mCurrentBlockLeftByte;
      }
      MappedByteBuffer out = 
          mLocalFileChannel.map(MapMode.READ_WRITE, mCurrentBlockWrittenByte, addByte);
      out.put(buf, 0, addByte);
      mCurrentBlockWrittenByte += addByte;
      mCurrentBlockLeftByte -= addByte;

      if (addByte < length) {
        getNextBlock();
        int moreAddByte = length - addByte;
        out = mLocalFileChannel.map(MapMode.READ_WRITE, mCurrentBlockWrittenByte, moreAddByte);
        out.put(buf, addByte, moreAddByte);
        mCurrentBlockWrittenByte += moreAddByte;
        mCurrentBlockLeftByte -= moreAddByte;
      }
    }
  }

  private void closeCurrentBlock() throws IOException {
    if (mLocalFileChannel != null) {
      mLocalFileChannel.close();
      mLocalFile.close();
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockId != -1) {
      if (mCurrentBlockLeftByte != 0) {
        throw new IOException("The current block still has space left, no need to get new block");
      }
      closeCurrentBlock();
      mPreviousBlockIds.add(mCurrentBlockId);
    }

    mCurrentBlockId = TFS.getBlockIdBasedOnOffset(FID, mCurrentBlockWrittenByte);
    mCurrentBlockWrittenByte = 0;
    mCurrentBlockLeftByte = TFS.getBlockSizeByte(FID);

    if (WRITE_TYPE.isCache()) {
      if (!TFS.hasLocalWorker()) {
        mLocalFileChannel = null;
        mLocalFile = null;
        if (WRITE_TYPE.isMustCache()) {
          throw new IOException("No local worker on this machine.");
        }
        return;
      }
      File localFolder = TFS.createAndGetUserTempFolder();
      if (localFolder == null) {
        mLocalFileChannel = null;
        mLocalFile = null;
        if (WRITE_TYPE.isMustCache()) {
          throw new IOException("Failed to create temp user folder for tachyon client.");
        }
        return;
      }
      String localFilePath = localFolder.getPath() + "/" + mCurrentBlockId;
      mLocalFile = new RandomAccessFile(localFilePath, "rw");
      mLocalFileChannel = mLocalFile.getChannel();
      LOG.info("File " + localFilePath + " was created!");
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (mBuffer.position() >= USER_CONF.FILE_BUFFER_BYTES) {
      appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      mBuffer.clear();
    }

    mBuffer.put((byte) (b & 0xFF));
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

    if (mBuffer.position() + len >= USER_CONF.FILE_BUFFER_BYTES) {
      if (mBuffer.position() > 0) {
        appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
        mBuffer.clear();
      }

      if (len > 0) {
        appendCurrentBuffer(b, off, len);
      }
    } else {
      mBuffer.put(b, off, len);
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
    throw new IOException("Not implemented yet.");
  }

  public void cancel() throws IOException {
    mCancel = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (!mCancel && mBuffer.position() > 0) {
        appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      }

      if (mLocalFileChannel != null) {
        mLocalFileChannel.close();
        mLocalFile.close();
      }

      if (mCancel) {
        if (WRITE_TYPE.isCache()) {
          try {
            if (mCurrentBlockId != -1) {
              mPreviousBlockIds.add(mCurrentBlockId);
            }
            for (int k = 0; k < mPreviousBlockIds.size(); k ++) {
              TFS.cacheBlock(mPreviousBlockIds.get(k));
            }
          } catch (IOException e) {
            if (WRITE_TYPE == WriteType.CACHE) {
              throw e;
            }
          }
        }

        if (WRITE_TYPE.isThrough()) {
          // TODO activately delete the partial file in the underlayer fs.
        }
        TFS.releaseSpace(mWrittenBytes);
      } else {
        if (WRITE_TYPE.isCache()) {
          try {
            if (mCurrentBlockId != -1) {
              mPreviousBlockIds.add(mCurrentBlockId);
            }
            for (int k = 0; k < mPreviousBlockIds.size(); k ++) {
              TFS.cacheBlock(mPreviousBlockIds.get(k));
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
        }

        TFS.completeFile(FID);
      }
    }
    mClosed = true;
  }
}
