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

package alluxio.worker.block.reviewer;

import alluxio.annotation.PublicApi;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.StorageDirView;

/**
 * (Experimental) The API is subject to change in the future.
 * Interface for the review policy of allocation decision made by {@link Allocator}.
 *
 * Each {@link Allocator} has a Reviewer instance according to the policy definition.
 * For each block allocation decision, the Reviewer reviews it according to the criteria
 * defined in the policy.
 * If the allocation does not meet the criteria, the Reviewer will reject it.
 * */
@PublicApi
public interface Reviewer {
  /**
   * Reviews an allocation proposed by the {@link Allocator}.
   * Returning true means the allocation is accepted.
   * Returning false meanes the allocation is rejected.
   *
   * @param dirView the storage dir that the block is allocated to
   * @return whether the allocation is accepted
   * */
  boolean acceptAllocation(StorageDirView dirView);

  /**
   * Factory for {@link Reviewer}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link Reviewer}.
     *
     * @return the generated {@link Reviewer}, it will be a {@link ProbabilisticBufferReviewer}
     *         by default
     */
    public static Reviewer create() {
      return CommonUtils.createNewClassInstance(
              ServerConfiguration.<Reviewer>getClass(PropertyKey.WORKER_REVIEWER_CLASS),
              new Class[] {}, new Object[] {});
    }
  }
}
