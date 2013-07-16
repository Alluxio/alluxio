package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.thrift.ClientBlockInfo;

/**
 * <code>InputStream</code> interface implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 * 
 * TODO(Haoyuan) Further separate this into RemoteBlockInStream and LocalBlockInStream.
 */
public class BlockInStream extends InStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final int BLOCK_INDEX;

  private ClientBlockInfo mBlockInfo;
  private ByteBuffer mBuffer = null;
  private InputStream mCheckpointInputStream = null;
  private long mCheckpointReadByte;

  private boolean mRecache = true;
  private BlockOutStream mBlockOutStream = null;

  private boolean mClosed = false;

  /**
   * @param file
   * @param readType
   * @param blockIndex
   * @throws IOException
   */
  BlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    super(file, readType);

    BLOCK_INDEX = blockIndex;
    mBlockInfo = TFS.getClientBlockInfo(FILE.FID, BLOCK_INDEX);
    mCheckpointReadByte = 0;

    if (!FILE.isComplete()) {
      throw new IOException("File " + FILE.getPath() + " is not ready to read");
    }

    mBuffer = FILE.readByteBuffer(blockIndex);
    if (mBuffer == null && READ_TYPE.isCache()) {
      if (FILE.recache(blockIndex)) {
        mBuffer = FILE.readByteBuffer(blockIndex);
      }
      //      mBlockOutStream = new BlockOutStream();
      mRecache = true;
    }

    String checkpointPath = TFS.getCheckpointPath(FILE.FID);
    if (mBuffer == null && !checkpointPath.equals("")) {
      LOG.info("Will stream from underlayer fs: " + checkpointPath);
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath);
      try {
        mCheckpointInputStream = underfsClient.open(checkpointPath);
        mCheckpointInputStream.skip(mBlockInfo.offset);
      } catch (IOException e) {
        LOG.error("Failed to read from checkpoint " + FILE.FID);
        mCheckpointInputStream = null;
      }
    }

    if (mBuffer == null && mCheckpointInputStream == null) {
      throw new IOException("Can not find the block " + FILE + " " + BLOCK_INDEX);
    }
  }

  @Override
  public int read() throws IOException {
    if (mBuffer != null) {
      if (mBuffer.remaining() == 0) {
        close();
        return -1;
      }
      return mBuffer.get();
    }

    mCheckpointReadByte ++;
    if (mCheckpointReadByte > mBlockInfo.length) {
      return -1;
    }
    return mCheckpointInputStream.read();
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

    if (mBuffer != null) {
      int ret = Math.min(len, mBuffer.remaining());
      if (ret == 0) {
        close();
        return -1;
      }
      mBuffer.get(b, off, ret);
      return ret;
    }

    long ret = mBlockInfo.length - mCheckpointReadByte;
    if (ret < len) {
      ret = mCheckpointInputStream.read(b, off, (int) ret);
      mCheckpointReadByte += ret;
      return (int) ret;
    }

    ret = mCheckpointInputStream.read(b, off, len);
    mCheckpointReadByte += ret;
    return (int) ret;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mBuffer != null) {
        FILE.releaseBlockLock(BLOCK_INDEX);
      }
      if (mCheckpointInputStream != null) {
        mCheckpointInputStream.close();
      }
    }
    mClosed = true;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    if (mBuffer != null) {
      int ret = mBuffer.remaining();
      if (ret > n) {
        ret = (int) n;
      }
      mBuffer.position(mBuffer.position() + ret);
      mCheckpointReadByte += ret;
      return ret;
    }

    long ret = mBlockInfo.length - mCheckpointReadByte;
    if (ret > n) {
      ret = n;
    }

    long tmp = mCheckpointInputStream.skip(ret);
    ret = Math.min(ret, tmp);
    mCheckpointReadByte += ret;

    return ret;
  }
}
