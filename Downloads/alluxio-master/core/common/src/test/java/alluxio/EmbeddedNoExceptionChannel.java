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

package alluxio;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * A special version of {@link EmbeddedChannel} that doesn't fail on exception so that we can
 * still check result after the channel is closed.
 */
public final class EmbeddedNoExceptionChannel extends EmbeddedChannel {
  /**
   * @param handlers the handlers
   */
  public EmbeddedNoExceptionChannel(ChannelHandler... handlers) {
    super(handlers);
  }

  @Override
  public void checkException() {
  }
}
