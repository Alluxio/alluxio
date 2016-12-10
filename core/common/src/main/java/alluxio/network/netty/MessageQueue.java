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

package alluxio.network.netty;

import alluxio.Constants;

import com.google.common.base.Preconditions;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a message queue used to communicate between producers and consumers.
 *
 * Special properties of this queue implementation:
 * 1. It provides an interface to accept throwable from any thread (not necessarily the
 * producer and consumer). Once a throwable is accepted, all the messages in the queue will be
 * cleared, the producer and consumer threads will be waken up and stopped soon after that.
 * 2. Each implementation needs to implement a mechanism to notify the producer to produce messages
 * to the queue.
 * 3. The producer can optionally mark the queue as done (eof) so that future notifications to the
 * producer are ignored.
 *
 * @param <T> the message type
 */
@ThreadSafe
public abstract class MessageQueue<T> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Options mOptions;

  private ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  private Queue<T> mMessagesQueue = new LinkedList<>();
  @GuardedBy("mLock")
  // If there an exception caught, all the consumers and producers will be stopped.
  private Throwable mThrowable = null;
  @GuardedBy("mLock")
  // Whether the reached the last message.
  private boolean mEof = false;
  private final Condition mNotEmpty = mLock.newCondition();
  private final Condition mNotFull;

  /**
   * Options to initialize a {@link MessageQueue}.
   */
  public static final class Options {
    /** The max capacity. */
    private int mMaxCapacity = 4;

    /** Whether blocks on {@link MessageQueue#offerMessage(Object, boolean)} if the queue is full.*/
    private boolean mBlockOnOffer = false;

    /** Timeout for polling from the queue. */
    private int mPollTimeoutMs = Constants.MINUTE_MS;

    /** Timeout for offering into the queue. */
    private int mOfferTimeoutMs = 60 * Constants.MINUTE_MS;

    /**
     * @return the max capacity
     */
    public int getMaxCapacity() {
      return mMaxCapacity;
    }

    /**
     * @return whether to block on offer if the queue is full
     */
    public boolean isBlockOnOffer() {
      return mBlockOnOffer;
    }

    /**
     * @return the polling timeout
     */
    public int getPollTimeoutMs() {
      return mPollTimeoutMs;
    }

    /**
     * @return the offering timeout
     */
    public int getOfferTimeoutMs() {
      return mOfferTimeoutMs;
    }

    /**
     * @param maxCapacity the max capacity
     * @return the updated object
     */
    public Options setMaxCapacity(int maxCapacity) {
      Preconditions.checkArgument(maxCapacity >= 1);
      mMaxCapacity = maxCapacity;
      return this;
    }

    /**
     * @param blockOnOffer whether to block if the queue is full
     * @return the updated object
     */
    public Options setBlockOnOffer(boolean blockOnOffer) {
      mBlockOnOffer = blockOnOffer;
      return this;
    }

    /**
     * @param pollTimeoutMs the polling timeout
     * @return the updated object
     */
    public Options setPollTimeoutMs(int pollTimeoutMs) {
      mPollTimeoutMs = pollTimeoutMs;
      return this;
    }

    /**
     * @param offerTimeoutMs the offering timeout
     * @return the updated object
     */
    public Options setOfferTimeoutMs(int offerTimeoutMs) {
      mOfferTimeoutMs = offerTimeoutMs;
      return this;
    }

    private Options() {
    }  // prevents instantiation

    /**
     * @return the default option
     */
    public static Options defaultOptions() {
      return new Options();
    }
  }
  /**
   * Creates an instance of the queue.
   *
   * @param options the options
   */
  public MessageQueue(Options options) {
    mOptions = options;
    if (mOptions.isBlockOnOffer()) {
      mNotFull = mLock.newCondition();
    } else {
      mNotFull = null;
    }
  }

  /**
   * Polls a message from the queue. The caller takes ownership of this message. Makes sure to
   * release the message if it is an instance of {@link io.netty.util.ReferenceCounted}.
   *
   * @return the polled message
   * @throws EOFException this should not happen if the user checks the polled message and decides
   *         not to poll from the queue once EOF is reached. But if the user doesn't do that,
   *         {@link EOFException} is thrown to indicate that EOF is reached.
   * @throws TimeoutException if it times out while polling from the queue
   * @throws Throwable any exception that passed from
   *         {@link MessageQueue#exceptionCaught(Throwable)}
   */
  public T pollMessage() throws EOFException, TimeoutException, Throwable {
    while (true) {
      try {
        mLock.lock();
        if (mThrowable != null) {
          throw mThrowable;
        }
        if (mMessagesQueue.isEmpty() && mEof) {
          throw new EOFException("Reached the end of the block.");
        }
        signalIfNecessary();
        if (!mMessagesQueue.isEmpty()) {
          return mMessagesQueue.poll();
        } else {
          try {
            if (!mNotEmpty.await(mOptions.getPollTimeoutMs(), TimeUnit.MILLISECONDS)) {
              throw new TimeoutException("pollMessage timeout.");
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        signalIfNecessary();
      } finally {
        if (mNotFull != null) {
          mNotFull.signal();
        }
        mLock.unlock();
      }
    }
  }

  /**
   * Resets the queue. Before calling this, the caller should guarantee that all the pending producers
   * and consumers are stopped.
   * If you are not confident, it is better to create a new instance of this queue after calling
   * {@link MessageQueue#reset()}.
   */
  public void reset() {
    try {
      mLock.lock();
      while (!mMessagesQueue.isEmpty()) {
        T message = mMessagesQueue.poll();
        ReferenceCountUtil.release(message);
      }
      mThrowable = null;
      mEof = false;
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Enqueues a message to the queue. If the queue is configured to be blocking if the queue is
   * full, the operation will block untill the queue has available space.
   *
   * If the message is a instance of {@link io.netty.util.ReferenceCounted}, the queue takes
   * ownership of that.
   *
   * @param message the message to enqueue
   * @param eof whether this is the last packet that should enqueued
   */
  public void offerMessage(T message, boolean eof) throws Throwable {
    try {
      mLock.lock();
      while (true) {
        if (mThrowable != null) {
          throw mThrowable;
        }

        if (mNotFull == null || mMessagesQueue.size() < mOptions.getMaxCapacity()) {
          mMessagesQueue.offer(message);
          Preconditions.checkState(!mEof, "Reading message after reaching EOF.");
          mEof = eof;
          mNotEmpty.signal();
        } else {
          try {
            if (!mNotFull.await(mOptions.getOfferTimeoutMs(), TimeUnit.MILLISECONDS)) {
              throw new RuntimeException("offerMessage timeout.");
            }
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
        }
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * An exception is caught. Wakes up all the producers and consumers. Clears all the messages.
   *
   * @param throwable the exception
   */
  public void exceptionCaught(Throwable throwable) {
    try {
      mLock.lock();
      if (mThrowable != null) {
        mThrowable = throwable;
      }
      while (!mMessagesQueue.isEmpty()) {
        T message = mMessagesQueue.poll();
        ReferenceCountUtil.release(message);
      }
      if (mNotFull != null) {
        mNotFull.signal();
      }
      mNotEmpty.signalAll();
      if (mNotFull != null) {
        mNotFull.signalAll();
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Wakes up the producer to producer more messages.
   */
  protected abstract void signal();

  /**
   * A helper for {@link MessageQueue#signal()} to skip calling signal if possible.
   */
  private void signalIfNecessary() {
    if (!mEof && mMessagesQueue.size() < mOptions.getMaxCapacity() && mThrowable == null) {
      signal();
    }
  }
}
