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

import com.codahale.metrics.Counter;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.Queue;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the context of a write request received from netty channel. This class serves the
 * shared states of the request and can be accessed concurrently by the netty thread and I/O thread.
 *
 * @param <T> type of the write request
 */
@ThreadSafe
public class WriteRequestContext<T extends WriteRequest> {

  /** The requests of this context. */
  private final T mRequest;

  /** The buffer for packets read from the channel. */
  @GuardedBy("AbstractWriteHandler#mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();

  /**
   * Set to true if the packet writer is active.
   *
   * The following invariants (happens-before orders) must be maintained:
   * 1. When mPacketWriterActive is true, it is guaranteed that mPackets is polled at least
   *    once after the lock is released. This is guaranteed even when there is an exception
   *    thrown when writing the packet.
   * 2. When mPacketWriterActive is false, it is guaranteed that mPackets won't be polled before
   *    before someone sets it to true again.
   *
   * The above are achieved by protecting it with "mLock". It is set to true when a new packet
   * is read when it is false. It set to false when one of the these is true: 1) The mPackets queue
   * is empty; 2) The write request is fulfilled (eof or cancel is received); 3) A failure occurs.
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  private boolean mPacketWriterActive;

  /**
   * The error seen in either the netty I/O thread (e.g. failed to read from the network) or the
   * packet writer thread (e.g. failed to write the packet).
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  private Error mError;

  /**
   * The next pos to queue to the buffer. This is only updated and used by the netty I/O thread.
   */
  private long mPosToQueue;
  /**
   * The next pos to write to the block worker. This is only updated by the packet writer
   * thread. The netty I/O reads this only for sanity check during initialization.
   *
   * Using "volatile" because we want any value change of this variable to be
   * visible across both netty and I/O threads, meanwhile only one updater means atomicity of
   * operations is unnecessary;
   */
  protected volatile long mPosToWrite;

  private Counter mCounter;

  /** This is set when EOF or CANCEL is received. This is only for sanity check. */
  private volatile boolean mDone;

  /**
   * @param request the write request
   */
  public WriteRequestContext(T request) {
    mRequest = request;
    mPosToQueue = 0;
    mPosToWrite = 0;
    mDone = false;
  }

  /**
   * @return the write request
   */
  public T getRequest() {
    return mRequest;
  }

  /**
   * @return the buffer for packets read from the channel
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public Queue<ByteBuf> getPackets() {
    return mPackets;
  }

  /**
   * @return whether this packet writer is still active
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public boolean isPacketWriterActive() {
    return mPacketWriterActive;
  }

  /**
   * @return the error
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  @Nullable
  public Error getError() {
    return mError;
  }

  /**
   * @return the next position to queue to the buffer
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public long getPosToQueue() {
    return mPosToQueue;
  }

  /**
   * @return the next position to write to the block worker
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public long getPosToWrite() {
    return mPosToWrite;
  }

  /**
   * @return metrics counter associated with this request
   */
  @Nullable
  public Counter getCounter() {
    return mCounter;
  }

  /**
   * @return true when the EOF or CANCEL is received, false otherwise
   */
  public boolean isDoneUnsafe() {
    return mDone;
  }

  /**
   * @param packetWriterActive whether the packet writer is active
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public void setPacketWriterActive(boolean packetWriterActive) {
    mPacketWriterActive = packetWriterActive;
  }

  /**
   * @param error the error
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public void setError(Error error) {
    mError = error;
  }

  /**
   * @param posToQueue the next position to queue to the buffer
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public void setPosToQueue(long posToQueue) {
    mPosToQueue = posToQueue;
  }

  /**
   * @param posToWrite the next position to write to the block worker
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public void setPosToWrite(long posToWrite) {
    mPosToWrite = posToWrite;
  }

  /**
   * @param done whether the EOF or CANCEL is received
   */
  public void setDoneUnsafe(boolean done) {
    mDone = done;
  }

  /**
   * @param counter counter to set
   */
  public void setCounter(Counter counter) {
    mCounter = counter;
  }
}
