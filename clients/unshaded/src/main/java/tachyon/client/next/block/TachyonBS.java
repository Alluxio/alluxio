package tachyon.client.next.block;

import tachyon.client.BlockInStream;
import tachyon.client.BlockOutStream;
import tachyon.client.next.ClientOptions;

public class TachyonBS {

  public static TachyonBS get() {
    // TODO: Implement me
    return null;
  }

  public void close() {
    // TODO: Implement me
  }

  public void deleteBlock(long blockId) {
    // TODO: Implement me
  }

  public void freeBlock(long blockId) {
    // TODO: Implement me
  }

  public BlockInfo getBlockInfo(long blockId) {
    // TODO: Implement me
    return null;
  }

  public BlockInStream getBlockInStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public BlockOutStream getBlockOutStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public boolean promote(long blockId) {
    // TODO: Implement me
    return false;
  }
}
