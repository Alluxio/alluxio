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

import alluxio.worker.block.meta.StorageDirView;

/**
 * An implementation of {@link Reviewer} that never rejects.
 * This is used to turn the Reviewer logic off.
 * */
public class AcceptingReviewer implements Reviewer {
  @Override
  public boolean acceptAllocation(StorageDirView dirView) {
    return true;
  }
}
