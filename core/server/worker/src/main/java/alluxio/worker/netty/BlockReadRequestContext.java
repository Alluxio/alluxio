package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.io.BlockReader;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context of {@link BlockReadRequest}.
 */
@NotThreadSafe
public final class BlockReadRequestContext extends ReadRequestContext<BlockReadRequest> {
  private BlockReader mBlockReader;

  public BlockReadRequestContext(Protocol.ReadRequest request) {
    super(new BlockReadRequest(request));
  }

  /**
   * @return block reader
   */
  @Nullable
  public BlockReader getBlockReader() {
    return mBlockReader;
  }

  /**
   * @param blockReader block reader to set
   */
  public void setBlockReader(BlockReader blockReader) {
    mBlockReader = blockReader;
  }
}
