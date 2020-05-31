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
import net.jcip.annotations.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds results of a {@link BlockManagementTask}.
 *
 * It contains sub-results for each {@link BlockOperationType} that is issued
 * by the magement task.
 */
@NotThreadSafe
public class BlockManagementTaskResult {
  /** Map of block-operation results. */
  private Map<BlockOperationType, BlockOperationResult> mBlockOpResults;

  /**
   * Creates a new empty task result.
   */
  public BlockManagementTaskResult() {
    mBlockOpResults = new HashMap<>();
  }

  /**
   * Add results for a specific {@link BlockOperationType}.
   * Results will be merged, if this task result contains results for the given operation.
   *
   * @param opType block operation type
   * @param result block operation result
   * @return the updated task result
   */
  public BlockManagementTaskResult addOpResults(BlockOperationType opType,
      BlockOperationResult result) {
    if (!mBlockOpResults.containsKey(opType)) {
      mBlockOpResults.put(opType, new BlockOperationResult());
    }
    mBlockOpResults.get(opType).mergeWith(result);
    return this;
  }

  /**
   * @param opType block operation type
   * @return results for the given operation type (null if don't exist)
   */
  public BlockOperationResult getOperationResult(BlockOperationType opType) {
    return mBlockOpResults.get(opType);
  }

  /**
   * @return {@code true} if the task had no progress due to failures/back-offs
   */
  public boolean noProgress() {
    int opCount = 0;
    int failCount = 0;
    int backOffCount = 0;
    for (BlockOperationResult result : mBlockOpResults.values()) {
      opCount += result.opCount();
      failCount += result.failCount();
      backOffCount += result.backOffCount();
    }
    return opCount == failCount + backOffCount;
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper strHelper = MoreObjects.toStringHelper(this);
    for (Map.Entry<BlockOperationType, BlockOperationResult> entry : mBlockOpResults.entrySet()) {
      strHelper.add(entry.getKey().name(), entry.getValue());
    }
    return strHelper.toString();
  }
}
