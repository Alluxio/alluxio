package tachyon.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import tachyon.Constants;

public class FileInStream extends InStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final TachyonFS TFS;
  private final TachyonFile FILE;
  private final ReadType READ_TYPE;
  private final List<Long> BLOCKS;

  private long mCurrentPosition;
  private int mCurrentBlockIndex;
  private BlockInStream mCurrentBlockInStream;
  private Map<Integer, BlockInStream> mBlockInStreams;

  public FileInStream(TachyonFile file, ReadType opType, List<Long> blocks) {
    TFS = file.TFS;
    FILE = file;
    READ_TYPE = opType;
    BLOCKS = new ArrayList<Long>(blocks);

    mCurrentPosition = 0;
    mCurrentBlockIndex = -1;
    mCurrentBlockInStream = null;
  }

  @Override
  public int read() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int read(byte[] b) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public long skip(long n) throws IOException {
    long ret = n;
    if (mCurrentPosition + n >= FILE.length()) {
      ret = FILE.length() - mCurrentPosition;
      mCurrentPosition += ret;
    } else {
      mCurrentPosition += n;
    }

    if (n != 0) {
      int tBlockIndex = (int) (mCurrentPosition / FILE.getBlockSizeByte());
      if (tBlockIndex != mCurrentBlockIndex) {
        if (mCurrentBlockIndex != -1) {
          mCurrentBlockInStream.close();
        }

        mCurrentBlockIndex = tBlockIndex;
        mCurrentBlockInStream = createBlockInStream(mCurrentBlockIndex);
        long shouldSkip = mCurrentPosition % FILE.getBlockSizeByte();
        long skip = mCurrentBlockInStream.skip(shouldSkip);
        if (skip != shouldSkip) {
          throw new IOException("The underlayer BlockInStream only skip " + skip + 
              " instead of " + shouldSkip);
        }
      } else {
        long skip = mCurrentBlockInStream.skip(ret);
        if (skip != ret) {
          throw new IOException("The underlayer BlockInStream only skip " + skip + 
              " instead of " + ret);
        }
      }
    }

    return ret;
  }

  private BlockInStream createBlockInStream(int blockIndex) {
    // TODO Auto-generated method stub
    return null;
  }
}