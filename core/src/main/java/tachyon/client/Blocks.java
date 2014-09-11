package tachyon.client;

import java.io.IOException;

final class Blocks {
  private Blocks() {

  }

  public static Block createBlock(TachyonFS tachyonFS, long currentBlockId, long maxBlockSize)
      throws IOException {
    if (tachyonFS.hasLocalWorker()) {
      return new LocalBlock(tachyonFS, currentBlockId, maxBlockSize);
    }
    throw new IOException("The machine does not have any local worker.");
  }
}
