/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.Closeable;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A listener that will close the given resource when the operation completes. This class accepts
 * null resources.
 */
@ThreadSafe
final class ClosableResourceChannelListener implements ChannelFutureListener {
  private final Closeable mResource;

  /**
   * Creates a new instance of {@link ClosableResourceChannelListener}.
   *
   * @param resource the resource to close
   */
  ClosableResourceChannelListener(Closeable resource) {
    mResource = resource;
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception {
    if (mResource != null) {
      mResource.close();
    }
  }
}
