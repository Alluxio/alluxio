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

package alluxio.worker.grpc;

import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context of {@link BlockReadRequest}.
 */
@NotThreadSafe
public final class BlockReadRequestContext {
  /**
   * Set to true if the data reader is active. The following invariants must be maintained:
   * 1. If true, there will be at least one more message (data, eof or error) to be sent to gRPC.
   * 2. If false, there will be no more message sent to gRPC until it is set to true again.
   */
  private boolean mDataReaderActive;
  /**
   * The next pos to queue to the read buffer.
   */
  private long mPosToQueue;

  private long mPosReceived;
  /**
   * mEof, mCancel and mError are the notifications processed by the data reader thread. They can
   * be set by either the gRPC I/O thread or the data reader thread. mError overrides mCancel
   * and mEof, mEof overrides mCancel.
   *
   * These notifications determine 3 ways to complete a read request.
   * 1. mEof: The read request is fulfilled. All the data requested by the client or all the data in
   *    the block/file has been read. The data reader replies a SUCCESS response when processing
   *    mEof.
   * 2. mCancel: The read request is cancelled by the client. A cancel request is ignored if mEof
   *    is set. The data reader replies a CANCEL response when processing mCancel.
   *    Note: The client can send a cancel request after the server has sent a SUCCESS response. But
   *    it is not possible for the client to send a CANCEL request after the channel has been
   *    released. So it is impossible for a CANCEL request from one read request to cancel
   *    another read request.
   * 3. mError: mError is set whenever an error occurs. It can be from an exception when reading
   *    data, or writing data to stream or the client closes the channel etc. An ERROR response
   *    is optionally sent to the client when data reader thread process mError. The channel
   *    is closed after this error response is sent.
   *
   * Note: it is guaranteed that only one of SUCCESS and CANCEL responses is sent at most once
   * because the data reader thread won't be restarted as long as mCancel or mEof is set except
   * when error happens (mError overrides mCancel and mEof).
   */
  private boolean mEof;
  private boolean mCancel;
  private Error mError;
  private Counter mCounter;
  private Meter mMeter;
  /** This is set when the SUCCESS or CANCEL response is sent. This is only for sanity check. */
  private volatile boolean mDone;
  private BlockReader mBlockReader;
  /** The requests of this context. */
  private final BlockReadRequest mRequest;

  /**
   * @param request the read request
   */
  public BlockReadRequestContext(alluxio.grpc.ReadRequest request) {
    mRequest = new BlockReadRequest(request);
    mPosToQueue = 0;
    mDataReaderActive = false;
    mEof = false;
    mCancel = false;
    mError = null;
    mDone = false;
  }

  /**
   * @return request received from channel
   */
  public BlockReadRequest getRequest() {
    return mRequest;
  }

  /**
   * @return whether the data reader is active
   */
  @GuardedBy("BlockReadHandler#mLock")
  public boolean isDataReaderActive() {
    return mDataReaderActive;
  }

  /**
   * @return the next position to queue to the read buffer
   */
  @GuardedBy("BlockReadHandler#mLock")
  public long getPosToQueue() {
    return mPosToQueue;
  }

  /**
   * @return the position before which data are received by the client
   */
  @GuardedBy("BlockReadHandler#mLock")
  public long getPosReceived() {
    return mPosReceived;
  }

  /**
   * @return true when the data reader replies a SUCCESS response, false otherwise
   */
  @GuardedBy("BlockReadHandler#mLock")
  public boolean isEof() {
    return mEof;
  }

  /**
   * @return true when a CANCEL request is received by the client, false otherwise
   */
  @GuardedBy("BlockReadHandler#mLock")
  public boolean isCancel() {
    return mCancel;
  }

  /**
   * @return the error during this read request
   */
  @GuardedBy("BlockReadHandler#mLock")
  @Nullable
  public Error getError() {
    return mError;
  }

  /**
   * @return true when the SUCCESS or CANCEL response is sent, false otherwise
   */
  public boolean isDoneUnsafe() {
    return mDone;
  }

  /**
   * @return metrics counter associated with this request
   */
  @Nullable
  public Counter getCounter() {
    return mCounter;
  }

  /**
   * @return metrics meter associated with this request
   */
  @Nullable
  public Meter getMeter() {
    return mMeter;
  }

  /**
   * @param packetReaderActive packet reader state to set
   */
  @GuardedBy("BlockReadHandler#mLock")
  public void setDataReaderActive(boolean packetReaderActive) {
    mDataReaderActive = packetReaderActive;
  }

  /**
   * @param posToQueue the next position to queue to the netty buffer to set
   */
  @GuardedBy("BlockReadHandler#mLock")
  public void setPosToQueue(long posToQueue) {
    mPosToQueue = posToQueue;
  }

  /**
   * @param posReceived the position before which data are received by the client
   */
  @GuardedBy("BlockReadHandler#mLock")
  public void setPosReceived(long posReceived) {
    mPosReceived = posReceived;
  }

  /**
   * @param eof whether SUCCESS response is replied
   */
  @GuardedBy("BlockReadHandler#mLock")
  public void setEof(boolean eof) {
    mEof = eof;
  }

  /**
   * @param cancel whether the CANCEL request is received
   */
  @GuardedBy("BlockReadHandler#mLock")
  public void setCancel(boolean cancel) {
    mCancel = cancel;
  }

  /**
   * @param error the error
   */
  @GuardedBy("BlockReadHandler#mLock")
  public void setError(Error error) {
    mError = error;
  }

  /**
   * @param done whether the SUCCESS or CANCEL response is sent
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

  /**
   * @param meter meter to set
   */
  public void setMeter(Meter meter) {
    mMeter = meter;
  }

  /**
   * @return block reader
   */
  @Nullable
  public BlockReader getBlockReader() {
    return mBlockReader;
  }

  /**
   * @param blockReader block reader to set
   */
  public void setBlockReader(BlockReader blockReader) {
    mBlockReader = blockReader;
  }
}
