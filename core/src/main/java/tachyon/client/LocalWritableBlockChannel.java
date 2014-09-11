package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.util.CountingWritableByteChannel;

final class LocalWritableBlockChannel implements WritableBlockChannel {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final TachyonFS mTachyonFS;
  private final CountingWritableByteChannel mLocalFileChannel;
  private final RandomAccessFile mLocalFile;
  private final String mLocalFilePath;
  private final long mBlockId;

  public LocalWritableBlockChannel(TachyonFS tachyonFS, long blockId) throws IOException {
    mTachyonFS = tachyonFS;
    mBlockId = blockId;
    File localFolder = tachyonFS.createAndGetUserLocalTempFolder();
    if (localFolder == null) {
      throw new IOException("Failed to create temp user folder for tachyon client.");
    }

    mLocalFilePath = CommonUtils.concat(localFolder.getPath(), blockId);
    mLocalFile = new RandomAccessFile(mLocalFilePath, "rw");
    // change the permission of the temporary file in order that the worker can move it.
    CommonUtils.changeLocalFileToFullPermission(mLocalFilePath);
    // use the sticky bit, only the client and the worker can write to the block
    CommonUtils.setLocalFileStickyBit(mLocalFilePath);
    mLocalFileChannel = new CountingWritableByteChannel(mLocalFile.getChannel());
    LOG.info(mLocalFilePath + " was created!");
  }

  @Override
  public void cancel() throws IOException {
    free();

    mTachyonFS.releaseSpace(mLocalFileChannel.written());
    new File(mLocalFilePath).delete();
    LOG.info("Canceled output of block " + mBlockId + ", deleted local file " + mLocalFilePath);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!mTachyonFS.requestSpace(src.remaining())) {
      String msg =
          "Local tachyon worker does not have enough " + "space (" + src.remaining()
              + ") or no worker for " /* + mFile.mFileId + " " */ + mBlockId;

      throw new IOException(msg);
    }
    return mLocalFileChannel.write(src);
  }

  @Override
  public boolean isOpen() {
    return mLocalFileChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    free();

    mTachyonFS.cacheBlock(mBlockId);
  }

  private void free() throws IOException {
    mLocalFileChannel.close();
    mLocalFile.close();
  }
}
