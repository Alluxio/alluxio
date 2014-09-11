package tachyon.client;

final class Blocks {
  private Blocks() {

  }

  public static Block createBlock(TachyonFS tachyonFS, long currentBlockId, long maxBlockSize) {
    return new LocalBlock(tachyonFS, currentBlockId, maxBlockSize);
  }
}
