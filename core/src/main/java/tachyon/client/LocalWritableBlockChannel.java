package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * Writes all {@link #write(java.nio.ByteBuffer)} directly to the file system.
 */
final class LocalWritableBlockChannel implements WritableBlockChannel {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Closer mCloser = Closer.create();
  private final TachyonFS mTachyonFS;
  private final FileChannel mLocalFileChannel;
  private final AtomicLong mWritten = new AtomicLong(0);
  private final File mLocalFile;
  private final long mBlockId;
  private volatile boolean mCanWrite = true;

  LocalWritableBlockChannel(TachyonFS tachyonFS, long blockId) throws IOException {
    mTachyonFS = tachyonFS;
    mBlockId = blockId;
    File localFolder = tachyonFS.createAndGetUserLocalTempFolder();
    if (localFolder == null) {
      throw new IOException("Failed to create temp user folder for tachyon client.");
    }

    mLocalFile = new File(localFolder, new Long(blockId).toString());
    // old behavior allowed new block streams to be created even if the file exists
    // in order to fix this behavior, FileInStream.seek needs to be rethought about
    // since that tells the worker "block is good!" even though in some cases
    // it won't be. In order to move forward with remote write, leaving this behavior
    // unchanged and expecting a different PR will fix this behavior.
    // Sorry future self!

    // TODO reject blocks that exists locally in both temp and data dir
    // if (mLocalFile.exists()) {
    // throw new IOException("Block exists; unable to write new block");
    // }

    // RandomAccessFile will create the file, so must be before check permissions
    RandomAccessFile localFile = mCloser.register(new RandomAccessFile(mLocalFile, "rw"));
    // change the permission of the temporary file in order that the worker can move it.
    checkPermission();

    mLocalFileChannel = mCloser.register(localFile.getChannel());
    LOG.info("{} was created!", mLocalFile);
  }

  private void checkPermission() throws IOException {
    // change the permission of the file and use the sticky bit
    String path = mLocalFile.getAbsolutePath();
    CommonUtils.changeLocalFileToFullPermission(path);
    CommonUtils.setLocalFileStickyBit(path);
  }

  @Override
  public void cancel() throws IOException {
    free();

    mTachyonFS.releaseSpace(mWritten.get());
    mLocalFile.delete();
    LOG.info("Canceled output of block {}, deleted local file {}", mBlockId, mLocalFile);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!mCanWrite) {
      throw new IOException("Can not write cache.");
    }

    if (!mTachyonFS.requestSpace(src.remaining())) {
      mCanWrite = false;
      String msg =
          "Local tachyon worker does not have enough space (" + src.remaining()
              + ") or no worker for " + mLocalFile + " with block id " + mBlockId;

      throw new IOException(msg);
    }
    int written = src.position();
    ByteBuffer out =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, mWritten.get(),
            src.limit() - src.position());
    out.put(src);

    written = src.position() - written;

    mWritten.getAndAdd(written);

    return written;
  }

  @Override
  public boolean isOpen() {
    return mLocalFileChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (mCanWrite) {
      free();

      mTachyonFS.cacheBlock(mBlockId);
    } else {
      // we failed to write this block, so cancel it
      // this logic is different than BlockOutStream because that buffers and expects to always
      // succeed. When the buffer is smaller than the block size, and you fail after writing a block
      // then BlockOutStream will hit this case.
      cancel();
    }
  }

  /**
   * Free all locally consumed resources, such as streams.
   */
  private void free() throws IOException {
    mCloser.close();
  }
}
