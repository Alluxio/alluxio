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
import alluxio.worker.block.meta.StorageDirView;

@PublicApi
public interface Reviewer {
  boolean reviewAllocation(StorageDirView dirView);

  /**
   * Factory for {@link Reviewer}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    // TODO(jiacheng): Is it possible we need a list of reviewers in the future, each checking one criteria?
    /**
     * Factory for {@link Reviewer}.
     *
     * @return the generated {@link Reviewer}, it will be a {@link ProbabilisticBufferReviewer} by default
     */
    public static Reviewer create() {
      return CommonUtils.createNewClassInstance(
              ServerConfiguration.<Reviewer>getClass(PropertyKey.WORKER_REVIEWER_CLASS),
              new Class[] {}, new Object[] {});
    }
  }
}
