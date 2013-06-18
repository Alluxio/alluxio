package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;

public class BlockInStream extends InStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final TachyonFS TFS;
  private final TachyonFile FILE;
  private final long BLOCK_ID;
  private final ReadType READ_TYPE;

  private ByteBuffer mBuffer = null;
  private InputStream mCheckpointInputStream = null;

  private boolean mClosed = false;

  InStream(TachyonFile file, ReadType opType, long blockId) throws IOException {
    TFS = file.TFS;
    FILE = file;
    BLOCK_ID = blockId;
    READ_TYPE = opType;

    if (!FILE.isReady()) {
      throw new IOException("File " + FILE.getPath() + " is not ready to read");
    }

    mBuffer = FILE.readByteBuffer();
    if (mBuffer == null && READ_TYPE.isCache()) {
      if (FILE.recache()) {
        mBuffer = FILE.readByteBuffer();
      }
    }

    String checkpointPath = TFS.getCheckpointPath(FID);
    if (mBuffer == null && !checkpointPath.equals("")) {
      LOG.info("Will stream from underlayer fs: " + checkpointPath);
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath);
      try {
        mCheckpointInputStream = underfsClient.open(checkpointPath);
      } catch (IOException e) {
        LOG.error("Failed to read from checkpoint " + FID);
        mCheckpointInputStream = null;
      }
    }
    if (mBuffer == null && mCheckpointInputStream == null) {
      throw new IOException("Can not find the file " + FILE.getPath());
    }
  }

  @Override
  public int read() throws IOException {
    try {
      return mBuffer.get();
    } catch (java.nio.BufferUnderflowException e) {
      close();
      return -1;
    } catch (NullPointerException e) {
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

    return mCheckpointInputStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mBuffer != null) {
        FILE.releaseFileLock();
      }
      if (mCheckpointInputStream != null) {
        mCheckpointInputStream.close();
      }
    }
    mClosed = true;
  }
}
