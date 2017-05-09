package alluxio.worker.netty;

import alluxio.util.IdUtils;
import alluxio.worker.SessionCleanable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A handler which is associated with a session id and {@link SessionCleanable} objects. In the
 * event of the channel being closed, the cleanup method will be invoked for each
 * {@link SessionCleanable}.
 *
 * Extending classes can override {@link #channelUnregistered} to clean up any additional state
 * being held.
 */
abstract class DataServerSessionHandler extends ChannelInboundHandlerAdapter {
  /** Components which need to be updated when the session is destroyed. */
  private final SessionCleanable[] mCleanables;
  /** The session id of this handler. */
  protected final long mSessionId;

  protected DataServerSessionHandler(SessionCleanable... sessionCleanables) {
    mCleanables = sessionCleanables;
    mSessionId = IdUtils.getRandomNonNegativeLong();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    for (SessionCleanable cleanable : mCleanables) {
      cleanable.cleanupSession(mSessionId);
    }
    ctx.fireChannelUnregistered();
  }
}
