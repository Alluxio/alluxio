package tachyon.worker.block.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.worker.block.meta.BlockMeta;

/**
 * This class provides write access to a block data file locally stored in managed storage.
 * <p>
 * This class does not provide thread-safety. Corresponding lock must be acquired.
 */
public class LocalFileBlockWriter implements BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();

  public LocalFileBlockWriter(BlockMeta blockMeta) throws IOException {
    this(Preconditions.checkNotNull(blockMeta).getPath());
  }

  public LocalFileBlockWriter(String path) throws IOException {
    mFilePath = Preconditions.checkNotNull(path);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "w"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public WritableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    return write(mLocalFileChannel.size(), inputBuf);
  }

  /**
   * Writes data to the block from an input ByteBuffer.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf ByteBuffer that input data is stored in
   * @return the size of data that was written
   * @throws IOException
   */
  private long write(long offset, ByteBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.limit();
    ByteBuffer outputBuf =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    outputBuf.put(inputBuf);
    CommonUtils.cleanDirectBuffer(outputBuf);
    return outputBuf.limit();
  }

  @Override
  public boolean delete() {
    return new File(mFilePath).delete();
  }

  @Override
  public boolean move(String dstPath) {
    // Check if destPath equals the current path, in this case, do nothing.
    if (mFilePath.equals(dstPath)) {
      return true;
    }
    return new File(mFilePath).renameTo(new File(dstPath));
  }
}