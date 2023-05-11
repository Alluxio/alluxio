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

package alluxio.job;

import alluxio.Constants;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Metadata for an Alluxio worker.
 */
@ThreadSafe
public final class MasterWorkerInfo {
  /** Worker's address. */
  private final WorkerNetAddress mWorkerAddress;
  /** The id of the worker. */
  private final long mId;
  /** Start time of the worker in ms. */
  private final long mStartTimeMs;
  /** Worker's last updated time in ms. */
  private long mLastUpdatedTimeMs;

  /**
   * Creates a new instance of {@link MasterWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public MasterWorkerInfo(long id, WorkerNetAddress address) {
    mWorkerAddress = Preconditions.checkNotNull(address);
    mId = id;
    mStartTimeMs = System.currentTimeMillis();
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * @return the worker's address
   */
  public synchronized WorkerNetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the id of the worker
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the worker in ms
   */
  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * @return the start time in milliseconds
   */
  public synchronized long getStartTime() {
    return mStartTimeMs;
  }

  /**
   * @return generated {@link WorkerInfo} for this worker
   */
  public synchronized WorkerInfo generateClientWorkerInfo() {
    return new WorkerInfo().setId(mId).setAddress(mWorkerAddress).setLastContactSec(
        (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS))
        .setState("In Service").setStartTimeMs(mStartTimeMs);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("MasterWorkerInfo(");
    sb.append(" ID: ").append(mId);
    sb.append(", mWorkerAddress: ").append(mWorkerAddress);
    sb.append(", mLastUpdatedTimeMs: ").append(mLastUpdatedTimeMs);
    return sb.toString();
  }

  /**
   * Updates the last updated time of the worker in ms.
   */
  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }
}
