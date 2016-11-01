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

package alluxio.client.block.stream;

import alluxio.Constants;
import alluxio.client.block.BlockStoreContext;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * The interface to read remote block from data server.
 */
public abstract class NettyBlockReader implements BlockReader {
  private Channel mChannel = null;

  public static class PacketQueue {
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
    private Channel mChannel = null;
    private final int mMaxCapacity = 4;
    // TODO: make the size configurable.
    private ReentrantLock mLock = new ReentrantLock();
    @GuardedBy("mLock")
    private Queue<ByteBuf> mPacketsQueue = new LinkedList<>();
    private Queue<Throwable> mThrowableQueue = new LinkedList<>();
    // Will be set to true if we are done with reading the block range.
    private boolean mEof = false;
    private Condition mNotEmpty = mLock.newCondition();

    private final static int READ_TIMEOUT_MS = Constants.MINUTE_MS;

    public void read() {
      try {
        mLock.lock();
        if (!mEof && mPacketsQueue.size() < mMaxCapacity && mThrowableQueue.isEmpty()) {
          mChannel.read();
        }
      } finally {
        mLock.unlock();
      }
    }

    // The caller is responsible for releasing this.
    public ByteBuf pollPacket() throws IOException {
      while (true) {
        try {
          mLock.lock();
          if (!mPacketsQueue.isEmpty()) {
            return mPacketsQueue.poll();
          } else if (mEof) {
            throw new EOFException("Reached the end of the block.");
          } else if (!mThrowableQueue.isEmpty()) {
            // We only look at the first Exception.
            throw new IOException(mThrowableQueue.poll());
          } else {
            try {
              mNotEmpty.await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              throw new IOException(e);
            }
          }
        } finally {
          mLock.unlock();
        }
      }
    }

    public void offerPacket(ByteBuf packet, boolean eof) {
      try {
        mLock.lock();
        Preconditions.checkState(!mEof, "Reading packet after reaching EOF.");
        mPacketsQueue.offer(packet);
        if (mPacketsQueue.size() > mMaxCapacity) {
          LOG.warn("Netty block reader queue size exceeds the maximum capacity: actual: {}, max: {}",
              mPacketsQueue.size(), mMaxCapacity);
        }
        mEof = eof;
      } finally {
        mLock.unlock();
      }
    }

    public void exceptionCaught(Throwable throwable) {
      try {
        mLock.lock();
        mThrowableQueue.offer(throwable);
      } finally {
        mLock.unlock();
      }
    }
  }

  private PacketQueue mPacketQueue = new PacketQueue();

  public NettyBlockReader() {
    // TODO: build the channel with correct handlers.
    // mChannel = BlockStoreContext.acquireNettyChannel();
    // mChannel.pipeline().addLast(handler);

    // Send read request.
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    mPacketQueue.read();
    return mPacketQueue.pollPacket();
  }

  @Override
  public void close() throws IOException {
    // BlockStoreContext.releaseNettyChannel();
    // Send stop msg.
    // wait for eof.
  }
}
