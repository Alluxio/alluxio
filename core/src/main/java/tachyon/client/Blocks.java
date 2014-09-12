package tachyon.client;

import java.io.IOException;

final class Blocks {
  private static final long DEFAULT_BUFFER_SIZE = 4096; // 4k

  private Blocks() {

  }

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
