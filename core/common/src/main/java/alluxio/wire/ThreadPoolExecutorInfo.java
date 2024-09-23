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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about the ThreadPoolExecutor.
 */
@NotThreadSafe
public final class ThreadPoolExecutorInfo {

  private String mName;
  private long mMax;
  private long mMin;
  private long mQueueSize;
  private long mCurrent;
  private long mActive;
  private long mCompleted;
  private long mRejected;

  /**
   * Creates a new instance of {@link ThreadPoolExecutorInfo}.
   */
  public ThreadPoolExecutorInfo() {
  }

  /**
   * @return the name of the thread pool
   */
  public String getName() {
    return mName;
  }

  /**
   * @param name the name of thread pool
   */
  public void setName(String name) {
    mName = name;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).addValue(mName).toString();
  }

  /**
   * Sets minimum core pool size of thread pool.
   * @param min min value
   */
  public void setMin(long min) {
    mMin = min;
  }

  /**
   * @return min value
   */
  public long getMin() {
    return mMin;
  }

  /**
   * Sets maximum size of thread pool.
   * @param max value
   */
  public void setMax(long max) {
    mMax = max;
  }

  /**
   * @return max value
   */
  public long getMax() {
    return mMax;
  }

  /**
   * Sets the active thread pool number.
   * @param active the active thread pool number
   */
  public void setActive(long active) {
    mActive = active;
  }

  /**
   * @return the active thread pool number
   */
  public long getActive() {
    return mActive;
  }

  /**
   * Sets the completed task number.
   * @param completed the completed task number
   */
  public void setCompleted(long completed) {
    mCompleted = completed;
  }

  /**
   * @return the completed task number
   */
  public long getCompleted() {
    return mCompleted;
  }

  /**
   * Sets the current task number.
   * @param current the current task number
   */
  public void setCurrent(long current) {
    mCurrent = current;
  }

  /**
   * @return the current task number
   */
  public long getCurrent() {
    return mCurrent;
  }

  /**
   * Sets the rejected task number.
   * @param rejected the rejected task number
   */
  public void setRejected(long rejected) {
    mRejected = rejected;
  }

  /**
   * @return the rejected task number
   */
  public long getRejected() {
    return mRejected;
  }

  /**
   * Sets the waiting queue size.
   * @param queueSize the waiting queue size
   */
  public void setQueueSize(long queueSize) {
    mQueueSize = queueSize;
  }

  /**
   * @return the waiting queue size
   */
  public long getQueueSize() {
    return mQueueSize;
  }
}
