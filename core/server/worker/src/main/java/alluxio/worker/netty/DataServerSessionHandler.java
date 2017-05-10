/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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

  /**
   * Creates a new instance of this class.
   *
   * @param sessionCleanables cleanables which should be updated on channel close
   */
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
