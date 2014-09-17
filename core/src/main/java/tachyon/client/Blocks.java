package tachyon.client;

import java.io.IOException;

import tachyon.conf.UserConf;

/**
 * Utility class for working with Blocks.
 */
final class Blocks {
  /**
   * Default client side buffer.
   */
  private static final int DEFAULT_BUFFER_SIZE = UserConf.get().FILE_BUFFER_BYTES;

  private Blocks() {

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
}
