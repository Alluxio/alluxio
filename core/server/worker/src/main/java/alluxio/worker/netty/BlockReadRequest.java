package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The internal representation of a block read request.
 */
@NotThreadSafe
public final class BlockReadRequest extends ReadRequest {
  private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
  private final boolean mPromote;

  /**
   * Creates an instance of {@link BlockReadRequest}.
   *
   * @param request the block read request
   */
  BlockReadRequest(Protocol.ReadRequest request) {
    super(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
        request.getPacketSize());

    if (request.hasOpenUfsBlockOptions()) {
      mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
    } else {
      mOpenUfsBlockOptions = null;
    }
    mPromote = request.getPromote();
    // Note that we do not need to seek to offset since the block worker is created at the offset.
  }

  /**
   * @return if the block read type indicate promote in tier storage
   */
  public boolean isPromote() {
    return mPromote;
  }

  /**
   * @return the option to open UFS block
   */
  public Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions() {
    return mOpenUfsBlockOptions;
  }

  /**
   * @return true if the block is persisted in UFS
   */
  public boolean isPersisted() {
    return mOpenUfsBlockOptions != null && mOpenUfsBlockOptions.hasUfsPath();
  }

}
