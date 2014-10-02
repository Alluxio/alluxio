package tachyon.worker.hierarchy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;

final class BlockChannel implements SeekableByteChannel {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private boolean mPermission = false;
  private final String mFilePath;
  private final Closer mCloser = Closer.create();
  private final AtomicLong mPosition = new AtomicLong(0);

  BlockChannel(final String filePath) throws FileNotFoundException {
    mFilePath = Preconditions.checkNotNull(filePath);
    LOG.debug("{} is created", mFilePath);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public int read(ByteBuffer buffer) throws IOException {
    long fileLength = mLocalFile.length();
    int length = Math.min(buffer.remaining(), (int) (fileLength - mPosition.get()));

    ByteBuffer buf =
        mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, mPosition.getAndAdd(length), length);
    buffer.put(buf);
    return length;
  }

  @Override
  public int write(ByteBuffer srcBuf) throws IOException {
    checkPermission();
    int bufLen = srcBuf.remaining();
    int size = srcBuf.position();
    ByteBuffer out = mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, mPosition.get(), bufLen);
    out.put(srcBuf);
    size = srcBuf.position() - size;
    mPosition.getAndAdd(size);

    return size;
  }

  @Override
  public boolean isOpen() {
    return mLocalFileChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  private void checkPermission() throws IOException {
    if (!mPermission) {
      // change the permission of the file and use the sticky bit
      CommonUtils.changeLocalFileToFullPermission(mFilePath);
      CommonUtils.setLocalFileStickyBit(mFilePath);
      mPermission = true;
    }
  }

  @Override
  public long position() {
    return mPosition.get();
  }

  @Override
  public SeekableByteChannel position(long newPosition) {
    mPosition.set(newPosition);
    return this;
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    return mLocalFileChannel.transferTo(position, count, target);
  }
}
