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

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.StringUtil;

import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Special versions of {@link EmbeddedChannel}.
 */
public final class EmbeddedChannels {
  private EmbeddedChannels() {}  // prevent instantiation

  /**
   * A customized {@link EmbeddedChannel} that supports empty constructor and supports thread safe
   * read/write outbound messages from/to this channel.
   */
  public static final class EmbeddedEmptyCtorChannel extends EmbeddedChannel {
    private final EmbeddedChannelPipeline mPipeline;

    public EmbeddedEmptyCtorChannel() {
      // Invoke the parent ctor with a dummy handler.
      super(new ChannelInboundHandlerAdapter());
      // Reset the pipeline created from super constructor
      ChannelPipeline p = super.pipeline();
      // Remove the dummy handler.
      p.removeFirst();
      // Add a LastInboundHandler instance of this class
      p.addLast(new LastInboundHandler());
      mPipeline = new EmbeddedChannelPipeline(p);
    }

    @Override
    public ChannelPipeline pipeline() {
      if (mPipeline == null) {
        // called from the constructor of super class
        return super.pipeline();
      }
      return mPipeline;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
      synchronized (this) {
        super.doWrite(in);
      }
    }

    @Override
    public Object readOutbound() {
      synchronized (this) {
        return super.readOutbound();
      }
    }

    private void recordException(Throwable e) {
      try {
        Method method = getClass().getSuperclass().getDeclaredMethod("recordException");
        method.setAccessible(true);
        method.invoke(this, e);
      } catch (Exception ee) {
        throw new RuntimeException(ee);
      }
    }

    @ChannelHandler.Sharable
    private final class LastInboundHandler extends ChannelInboundHandlerAdapter {
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        inboundMessages().add(msg);
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        recordException(cause);
      }
    }

    // TODO(binfan): rewrite this class after we support to use mockito 2 for tests. The only
    // purpose of creating the following class is to extend method "addLast" from
    // io.netty.channel.DefaultChannelPipeline which happens to be a final and package-private
    // class.
    /**
     * The channel pipeline that ensures a {@link LastInboundHandler} instance is always in the
     * end. This is a wrapper of the given pipeline instance with modified {@link #addLast} which
     * ensures the invariance.
     */
    public class EmbeddedChannelPipeline implements ChannelPipeline {
      private final ChannelPipeline mPipeline;

      /**
       * @param pipeline the underlying pipeline
       */
      public EmbeddedChannelPipeline(ChannelPipeline pipeline) {
        mPipeline = pipeline;
      }

      @Override
      public ChannelPipeline addFirst(String name, ChannelHandler handler) {
        mPipeline.addFirst(name, handler);
        return this;
      }

      @Override
      public ChannelPipeline addFirst(EventExecutorGroup group, String name,
          ChannelHandler handler) {
        mPipeline.addFirst(group, name, handler);
        return this;
      }

      @Override
      public ChannelPipeline addFirst(ChannelHandler... handlers) {
        mPipeline.addFirst(handlers);
        return this;
      }

      @Override
      public ChannelPipeline addFirst(EventExecutorGroup group, ChannelHandler... handlers) {
        mPipeline.addFirst(group, handlers);
        return this;
      }

      @Override
      public ChannelPipeline addLast(String name, ChannelHandler handler) {
        return addLast(null, name, handler);
      }

      @Override
      public ChannelPipeline addLast(EventExecutorGroup group, String name,
          ChannelHandler handler) {
        ChannelHandler last = mPipeline.removeLast();
        Preconditions.checkState(last instanceof LastInboundHandler);
        mPipeline.addLast(group, name, handler).addLast(last);
        return this;
      }

      @Override
      public ChannelPipeline addLast(ChannelHandler... handlers) {
        return addLast(null, handlers);
      }

      @Override
      public ChannelPipeline addLast(EventExecutorGroup executor, ChannelHandler... handlers) {
        if (handlers == null) {
          throw new NullPointerException("handlers");
        }
        for (ChannelHandler h: handlers) {
          if (h == null) {
            break;
          }
          addLast(executor, StringUtil.simpleClassName(h.getClass()) + "#0", h);
        }
        return this;
      }

      public ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler) {
        mPipeline.addBefore(baseName, name, handler);
        return this;
      }

      @Override
      public ChannelPipeline addBefore(EventExecutorGroup group, String baseName, String name,
          ChannelHandler handler) {
        mPipeline.addBefore(group, baseName, name, handler);
        return this;
      }

      public ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler) {
        mPipeline.addAfter(baseName, name, handler);
        return this;
      }

      @Override
      public ChannelPipeline addAfter(EventExecutorGroup group, String baseName, String name,
          ChannelHandler handler) {
        mPipeline.addAfter(group, baseName, name, handler);
        return this;
      }

      public ChannelPipeline remove(ChannelHandler handler) {
        mPipeline.remove(handler);
        return this;
      }

      public ChannelHandler remove(String name) {
        return mPipeline.remove(name);
      }

      public <T extends ChannelHandler> T remove(Class<T> handlerType) {
        return mPipeline.remove(handlerType);
      }

      @Override
      public ChannelHandler removeFirst() {
        return mPipeline.removeFirst();
      }

      @Override
      public ChannelHandler removeLast() {
        return mPipeline.removeLast();
      }

      public ChannelPipeline replace(ChannelHandler oldHandler, String newName,
          ChannelHandler newHandler) {
        mPipeline.replace(oldHandler, newName, newHandler);
        return this;
      }

      public ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
        return mPipeline.replace(oldName, newName, newHandler);
      }

      public <T extends ChannelHandler> T replace(Class<T> oldHandlerType, String newName,
          ChannelHandler newHandler) {
        return mPipeline.replace(oldHandlerType, newName, newHandler);
      }

      @Override
      public ChannelHandler first() {
        return mPipeline.first();
      }

      @Override
      public ChannelHandlerContext firstContext() {
        return mPipeline.firstContext();
      }

      @Override
      public ChannelHandler last() {
        return mPipeline.last();
      }

      @Override
      public ChannelHandlerContext lastContext() {
        return mPipeline.lastContext();
      }

      @Override
      public ChannelHandler get(String name) {
        return mPipeline.get(name);
      }

      @Override
      public <T extends ChannelHandler> T get(Class<T> handlerType) {
        return mPipeline.get(handlerType);
      }

      @Override
      public ChannelHandlerContext context(ChannelHandler handler) {
        return mPipeline.context(handler);
      }

      @Override
      public ChannelHandlerContext context(String name) {
        return mPipeline.context(name);
      }

      @Override
      public ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType) {
        return mPipeline.context(handlerType);
      }

      @Override
      public Channel channel() {
        return mPipeline.channel();
      }

      @Override
      public List<String> names() {
        return mPipeline.names();
      }

      public Map<String, ChannelHandler> toMap() {
        return mPipeline.toMap();
      }

      @Override
      public ChannelPipeline fireChannelRegistered() {
        mPipeline.fireChannelRegistered();
        return this;
      }

      @Override
      public ChannelPipeline fireChannelUnregistered() {
        mPipeline.fireChannelUnregistered();
        return this;
      }

      @Override
      public ChannelPipeline fireChannelActive() {
        mPipeline.fireChannelActive();
        return this;
      }

      @Override
      public ChannelPipeline fireChannelInactive() {
        mPipeline.fireChannelInactive();
        return this;
      }

      @Override
      public ChannelPipeline fireExceptionCaught(Throwable cause) {
        mPipeline.fireExceptionCaught(cause);
        return this;
      }

      @Override
      public ChannelPipeline fireUserEventTriggered(Object event) {
        mPipeline.fireUserEventTriggered(event);
        return this;
      }

      @Override
      public ChannelPipeline fireChannelRead(Object msg) {
        mPipeline.fireChannelRead(msg);
        return this;
      }

      @Override
      public ChannelPipeline fireChannelReadComplete() {
        mPipeline.fireChannelReadComplete();
        return this;
      }

      @Override
      public ChannelPipeline fireChannelWritabilityChanged() {
        mPipeline.fireChannelWritabilityChanged();
        return this;
      }

      @Override
      public ChannelFuture bind(SocketAddress localAddress) {
        return mPipeline.bind(localAddress);
      }

      @Override
      public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return mPipeline.bind(localAddress, promise);
      }

      @Override
      public ChannelFuture connect(SocketAddress remoteAddress) {
        return mPipeline.connect(remoteAddress);
      }

      @Override
      public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return mPipeline.connect(remoteAddress, localAddress);
      }

      @Override
      public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return mPipeline.connect(remoteAddress, promise);
      }

      @Override
      public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress,
          ChannelPromise promise) {
        return mPipeline.connect(remoteAddress, localAddress, promise);
      }

      @Override
      public ChannelFuture disconnect() {
        return mPipeline.disconnect();
      }

      @Override
      public ChannelFuture disconnect(ChannelPromise promise) {
        return mPipeline.disconnect(promise);
      }

      @Override
      public ChannelFuture close() {
        return mPipeline.close();
      }

      @Override
      public ChannelFuture close(ChannelPromise promise) {
        return mPipeline.close(promise);
      }

      @Override
      public ChannelFuture deregister() {
        return mPipeline.deregister();
      }

      @Override
      public ChannelFuture deregister(ChannelPromise promise) {
        return mPipeline.deregister(promise);
      }

      @Override
      public ChannelPipeline read() {
        mPipeline.read();
        return this;
      }

      @Override
      public ChannelFuture write(Object msg) {
        return mPipeline.write(msg);
      }

      @Override
      public ChannelFuture write(Object msg, ChannelPromise promise) {
        return mPipeline.write(msg, promise);
      }

      @Override
      public ChannelPipeline flush() {
        mPipeline.flush();
        return this;
      }

      @Override
      public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return mPipeline.writeAndFlush(msg, promise);
      }

      @Override
      public ChannelFuture writeAndFlush(Object msg) {
        return mPipeline.writeAndFlush(msg);
      }

      @Override
      public Iterator<Map.Entry<String, ChannelHandler>> iterator() {
        return mPipeline.iterator();
      }
    }
  }
}
