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

import alluxio.Constants;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * 1. single producer. single consumer.
 * 2. consumer doesn't offer anything (including exceptions)
 * 3. producer has to either set EOF or an exception to indicate
 * the end of the stream.
 * 4. Optionally, the packet and also contains a special msg indicating the
 * end of the stream.
 * @param <T>
 */
public abstract class PacketQueue<T> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final int mMaxCapacity = 4;
  // TODO: make the size configurable.
  private ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  private Queue<T> mPacketsQueue = new LinkedList<>();
  private Queue<Throwable> mThrowableQueue = new LinkedList<>();
  private boolean mEof = false;
  private Condition mNotEmpty = mLock.newCondition();

  private final static int POLL_TIMEOUT_MS = Constants.MINUTE_MS;

  protected abstract void signal();

  private void signalIfNecessary() {
    if (!mEof && mPacketsQueue.size() < mMaxCapacity && mThrowableQueue.isEmpty()) {
      signal();
    }
  }

  // The caller is responsible for releasing this.
  public T pollPacket() throws Throwable {
    while (true) {
      try {
        mLock.lock();
        signalIfNecessary();
        if (!mPacketsQueue.isEmpty()) {
          return mPacketsQueue.poll();
       } else if (!mThrowableQueue.isEmpty()) {
          // We only look at the first Exception.
          throw mThrowableQueue.poll();
        } else if (mEof) {
          throw new EOFException("Reached the end of the block.");
        } else {
          try {
            mNotEmpty.await(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
        signalIfNecessary();
      } finally {
        mLock.unlock();
      }
    }
  }

  public void clearPackets() {
    try {
      mLock.lock();
      while (!mPacketsQueue.isEmpty()) {
        T packet = mPacketsQueue.poll();
        ReferenceCountUtil.release(packet);
      }
    } finally {
      mLock.unlock();
    }
  }

  public void reset() {
    try {
      mLock.lock();
      while (!mPacketsQueue.isEmpty()) {
        T packet = mPacketsQueue.poll();
        ReferenceCountUtil.release(packet);
      }
      mThrowableQueue.clear();
      mEof = false;
    } finally {
      mLock.unlock();
    }
  }

  public void offerPacket(T packet, boolean eof) {
    try {
      mLock.lock();
      Preconditions.checkState(!mEof, "Reading packet after reaching EOF.");
      mPacketsQueue.offer(packet);
      if (mPacketsQueue.size() > mMaxCapacity) {
        LOG.warn("Netty block reader queue size exceeds the maximum capacity: actual: {}, max: {}",
            mPacketsQueue.size(), mMaxCapacity);
      }
      mEof = eof;
      mNotEmpty.signal();
    } finally {
      mLock.unlock();
    }
  }

  public void exceptionCaught(Throwable throwable) {
    try {
      mLock.lock();
      mThrowableQueue.offer(throwable);
      mNotEmpty.signal();
    } finally {
      mLock.unlock();
    }
  }
}
