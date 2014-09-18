package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.conf.UserConf;

/**
 * Utility class for working with Blocks.
 */
final class Blocks {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Default client side buffer.
   */
  private static final int DEFAULT_BUFFER_SIZE = UserConf.get().FILE_BUFFER_BYTES;

  private Blocks() {

  }

  /**
   * Attempts to create a new block channel for the given {@code blockIndex}. This block will reject
   * writes after {@link TachyonFile#getBlockSizeByte()} have been written.
   */
  public static WritableBlockChannel createWritableBlock(TachyonFile file, int blockIndex)
      throws IOException {
    long blockId = file.getBlockId(blockIndex);
    return createWritableBlock(file.mTachyonFS, blockId, file.getBlockSizeByte());
  }

  /**
   * Attempts to create a new block channel for the given {@code blockId}. This block will reject
   * writes after {@code maxBlockSize} have been written.
   */
  public static WritableBlockChannel createWritableBlock(TachyonFS tachyonFS, long blockId,
      long maxBlockSize) throws IOException {
    if (tachyonFS.hasLocalWorker()) {
      int bufferSize = (int) Math.min(maxBlockSize, DEFAULT_BUFFER_SIZE);
      LocalWritableBlockChannel local = new LocalWritableBlockChannel(tachyonFS, blockId);
      BufferedWritableBlockChannel buffered = new BufferedWritableBlockChannel(bufferSize, local);
      BoundedWritableBlockChannel bounded = new BoundedWritableBlockChannel(maxBlockSize, buffered);
      return bounded;
    }
    throw new IOException("The machine does not have any local worker.");
  }

  /**
   * Attempts to copy the {@link java.io.InputStream} into the channel. If the channel becomes full,
   * this method will return a {@link java.io.IOException}.
   */
  public static void copy(InputStream inputStream, WritableBlockChannel channel, long length)
      throws IOException {
    final byte buffer[] = new byte[UserConf.get().FILE_BUFFER_BYTES * 4];
    int limit;
    while (length > 0 && ((limit = inputStream.read(buffer)) >= 0)) {
      if (limit != 0) {
        if (length >= limit) {
          if (channel.write(ByteBuffer.wrap(buffer, 0, limit)) == limit) {
            length -= limit;
          } else {
            throw new IOException("Block channel full");
          }
        } else {
          if (channel.write(ByteBuffer.wrap(buffer, 0, (int) length)) == (int) length) {
            length = 0;
          } else {
            throw new IOException("Block channel full");
          }
        }
      }
    }
  }

  public static boolean existsLocally(TachyonFile file, int blockIndex) throws IOException {
    File localFolder = file.mTachyonFS.createAndGetUserLocalTempFolder();
    if (localFolder == null) {
      throw new IOException("Failed to create temp user folder for tachyon client.");
    }

    File localFile = new File(localFolder, new Long(file.getBlockId(blockIndex)).toString());
    return localFile.exists();
  }
}
