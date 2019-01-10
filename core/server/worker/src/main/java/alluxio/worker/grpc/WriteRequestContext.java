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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the context of a write request received from gRPC stream. This class serves the
 * shared states of the request and can be accessed concurrently by the gRPC event thread
 * and I/O thread.
 *
 * @param <T> type of the write request
 */
@ThreadSafe
public class WriteRequestContext<T extends WriteRequest> {

  /** The requests of this context. */
  private final T mRequest;

  /**
   * The error seen in either the gRPC event threads (e.g. failed to read from the network) or the
   * data writer thread (e.g. failed to write the data).
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  private alluxio.worker.grpc.Error mError;

  /**
   * The next pos to queue to the buffer. This is only updated and used by the gRPC event thread.
   */
  private long mPos;

  private Counter mCounter;
  private Meter mMeter;

  /** This is set when EOF or CANCEL is received. This is only for sanity check. */
  private volatile boolean mDone;

  /**
   * @param request the write request
   */
  public WriteRequestContext(T request) {
    mRequest = request;
    mPos = 0;
    mDone = false;
  }

  /**
   * @return the write request
   */
  public T getRequest() {
    return mRequest;
  }

  /**
   * @return the error
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  @Nullable
  public alluxio.worker.grpc.Error getError() {
    return mError;
  }

  /**
   * @return the next position to write to the block worker
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public long getPos() {
    return mPos;
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
   * @return true when the EOF or CANCEL is received, false otherwise
   */
  public boolean isDoneUnsafe() {
    return mDone;
  }

  /**
   * @param error the error
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public void setError(alluxio.worker.grpc.Error error) {
    mError = error;
  }

  /**
   * @param posToWrite the next position to write to the block worker
   */
  @GuardedBy("AbstractWriteHandler#mLock")
  public void setPos(long posToWrite) {
    mPos = posToWrite;
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

  /**
   * @param meter meter to set
   */
  public void setMeter(Meter meter) {
    mMeter = meter;
  }
}
