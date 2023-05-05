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

package alluxio.worker.block.management;

import com.google.common.base.MoreObjects;

/**
 * Result container for unique block management sub-task.
 * Sub-tasks are defined by {@link BlockOperationType} enum.
 */
public class BlockOperationResult {
  /** How many operations. */
  private int mOpCount;
  /** How many failed. */
  private int mFailCount;
  /** How many backed off. */
  private int mBackoffCount;

  /**
   * Creates an empty operation result.
   */
  public BlockOperationResult() {
    this(0, 0, 0);
  }

  /**
   * Creates an operation result.
   *
   * @param opCount operation count
   * @param failCount failure count
   * @param backOffCount back-off count
   */
  public BlockOperationResult(int opCount, int failCount, int backOffCount) {
    mOpCount = opCount;
    mFailCount = failCount;
    mBackoffCount = backOffCount;
  }

  /**
   * Merges result counters of this result with the given result.
   *
   * @param otherResult the result to merge with
   * @return the updated block operation result
   */
  public BlockOperationResult mergeWith(BlockOperationResult otherResult) {
    mOpCount += otherResult.mOpCount;
    mFailCount += otherResult.mFailCount;
    mBackoffCount += otherResult.mBackoffCount;
    return this;
  }

  /**
   * @return the operation count
   */
  public int opCount() {
    return mOpCount;
  }

  /**
   * @return the failure count
   */
  public int failCount() {
    return mFailCount;
  }

  /**
   * @return the back-off count
   */
  public int backOffCount() {
    return mBackoffCount;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("OpCount", mOpCount)
        .add("FailCount", mFailCount)
        .add("BackOffCount", mBackoffCount)
        .toString();
  }
}
