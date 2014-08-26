package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * BlockHandler for files on LocalFS, such as RamDisk, SSD and HDD.
 */
public final class BlockHandlerLocal extends BlockHandler {

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private boolean mPermission = false;
  private final String mFilePath;
  private final Closer mCloser = Closer.create();

  BlockHandlerLocal(String filePath) throws IOException {
    mFilePath = Preconditions.checkNotNull(filePath);
    LOG.debug(mFilePath + " is created");
    mLocalFile = new RandomAccessFile(mFilePath, "rw");
    mLocalFileChannel = mLocalFile.getChannel();
    mCloser.register(mLocalFile);
    mCloser.register(mLocalFileChannel);
  }

  @Override
  public int append(long blockOffset, ByteBuffer srcBuf) throws IOException {
    checkPermission();
    ByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, blockOffset, srcBuf.limit());
    out.put(srcBuf);

    return srcBuf.limit();
  }

  @Override
  protected void checkPermission() throws IOException {
    if (!mPermission) {
      // change the permission of the file and use the sticky bit
      CommonUtils.changeLocalFileToFullPermission(mFilePath);
      CommonUtils.setLocalFileStickyBit(mFilePath);
      mPermission = true;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  @Override
  public boolean delete() throws IOException {
    checkPermission();
    return new File(mFilePath).delete();
  }

  @Override
  public ByteBuffer read(long blockOffset, int length) throws IOException {
    long fileLength = mLocalFile.length();
    String error = null;
    if (blockOffset > fileLength) {
      error =
          String.format("blockOffset(%d) is larger than file length(%d)", blockOffset, fileLength);
    }
    if (error == null && length != -1 && blockOffset + length > fileLength) {
      error =
          String.format("blockOffset(%d) plus length(%d) is larger than file length(%d)",
              blockOffset, length, fileLength);
    }
    if (error != null) {
      throw new IllegalArgumentException(error);
    }
    if (length == -1) {
      length = (int) (fileLength - blockOffset);
    }
    ByteBuffer buf = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, blockOffset, length);
    return buf;
  }
}
