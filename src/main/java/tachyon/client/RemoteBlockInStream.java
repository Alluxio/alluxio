package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.thrift.ClientBlockInfo;

/**
 * BlockInStream for remote block.
 */
public class RemoteBlockInStream extends BlockInStream {
  private static final int BUFFER_SIZE = Constants.MB;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private ClientBlockInfo mBlockInfo;
  private InputStream mCheckpointInputStream = null;
  private long mCheckpointReadByte;
  private ByteBuffer mCurrentBuffer = null; 

  private boolean mRecache = true;
  private BlockOutStream mBlockOutStream = null;

  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    super(file, readType, blockIndex);

    mBlockInfo = TFS.getClientBlockInfo(FILE.FID, BLOCK_INDEX);
    mCheckpointReadByte = 0;

    if (!FILE.isComplete()) {
      throw new IOException("File " + FILE.getPath() + " is not ready to read");
    }

    //    mTachyonBuffer = FILE.readByteBuffer(blockIndex);
    //    if (mTachyonBuffer == null && READ_TYPE.isCache()) {
    //      if (FILE.recache(blockIndex)) {
    //        mTachyonBuffer = FILE.readByteBuffer(blockIndex);
    //      }
    //      //      mBlockOutStream = new BlockOutStream();
    //      mRecache = true;
    //    }

    mRecache = readType.isCache();
    if (mRecache) {
      mBlockOutStream = new BlockOutStream(file, WriteType.TRY_CACHE, blockIndex);
    }

    String checkpointPath = TFS.getCheckpointPath(FILE.FID);
    if (!checkpointPath.equals("")) {
      LOG.info("Will stream from underlayer fs: " + checkpointPath);
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath);
      try {
        mCheckpointInputStream = underfsClient.open(checkpointPath);
        long offset = mBlockInfo.offset;
        while (offset > 0) {
          long skipped = mCheckpointInputStream.skip(offset);
          offset -= skipped;
          if (skipped == 0) {
            throw new IOException("Failed to find the start position " + mBlockInfo.offset +
                " for block " + mBlockInfo);
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to read from checkpoint " + checkpointPath + " for File " + FILE.FID + 
            "\n" + e);
        mCheckpointInputStream = null;
      }
    }

    if (mCheckpointInputStream == null) {
      throw new IOException("Can not find the block " + FILE + " " + BLOCK_INDEX);
    }
  }

  private void doneRecache() throws IOException {
    if (mRecache) {
      mBlockOutStream.close();
    }
  }

  @Override
  public int read() throws IOException {
    mCheckpointReadByte ++;
    if (mCheckpointReadByte > mBlockInfo.length) {
      doneRecache();
      return -1;
    }
    int ret = mCheckpointInputStream.read();
    if (mRecache) {
      mBlockOutStream.write(ret);
    }
    return ret;
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    long ret = mBlockInfo.length - mCheckpointReadByte;
    if (ret < len) {
      len = (int) ret;
    }

    ret = mCheckpointInputStream.read(b, off, len);
    mCheckpointReadByte += ret;
    if (mRecache) {
      mBlockOutStream.write(b, off, (int) ret);
      if (mCheckpointReadByte == mBlockInfo.length) {
        doneRecache();
      }
    }
    return (int) ret;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mRecache) {
        mBlockOutStream.cancel();
      }
      mCheckpointInputStream.close();
    }
    mClosed = true;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long ret = mBlockInfo.length - mCheckpointReadByte;
    if (ret > n) {
      ret = n;
    }

    long tmp = mCheckpointInputStream.skip(ret);
    ret = Math.min(ret, tmp);
    mCheckpointReadByte += ret;

    if (ret > 0) {
      if (mRecache) {
        mBlockOutStream.cancel();
      }
      mRecache = false;
    }
    return ret;
  }
}
