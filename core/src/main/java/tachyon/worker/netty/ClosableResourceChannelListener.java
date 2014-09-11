package tachyon.worker.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.Closeable;

/**
 * A listener that will close the given resource when the operation completes. This class accepts
 * null resources.
 */
final class ClosableResourceChannelListener implements ChannelFutureListener {
  private final Closeable mResource;

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
