/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

/**
 * The worker space counter, in charge of counting and granting spaces in a worker daemon.
 */
public class WorkerSpaceCounter {
  private final long CAPACITY_BYTES;
  private long mUsedBytes;

  /**
   * @param capacityBytes
   *          The maximum memory space the TachyonWorker can use, in bytes
   */
  public WorkerSpaceCounter(long capacityBytes) {
    CAPACITY_BYTES = capacityBytes;
    mUsedBytes = 0;
  }

  /**
   * @return The available space size, in bytes
   */
  public synchronized long getAvailableBytes() {
    return CAPACITY_BYTES - mUsedBytes;
  }

  /**
   * @return The maximum memory space the TachyonWorker can use, in bytes
   */
  public long getCapacityBytes() {
    return CAPACITY_BYTES;
  }

  /**
   * @return The bytes that have been used
   */
  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * Request space
   * 
   * @param requestSpaceBytes
   *          The requested space size, in bytes
   * @return
   */
  public synchronized boolean requestSpaceBytes(long requestSpaceBytes) {
    if (getAvailableBytes() < requestSpaceBytes) {
      return false;
    }

    mUsedBytes += requestSpaceBytes;
    return true;
  }

  /**
   * Return used space size
   * 
   * @param returnUsedBytes
   *          The returned space size, in bytes
   */
  public synchronized void returnUsedBytes(long returnUsedBytes) {
    mUsedBytes -= returnUsedBytes;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("WorkerSpaceCounter(");
    sb.append(" TOTAL_BYTES: ").append(CAPACITY_BYTES);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(CAPACITY_BYTES - mUsedBytes);
    sb.append(" )");
    return sb.toString();
  }

  /**
   * Update the used bytes
   * 
   * @param usedBytes
   *          The new used bytes
   */
  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}