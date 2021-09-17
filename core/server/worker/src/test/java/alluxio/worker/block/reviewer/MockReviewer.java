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

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * An implementation of {@link Reviewer}. This is used for Reviewer unit test.
 * If a directory's `availableBytes` matches the bytes in `BYTES_TO_REJECT`,
 * allocation will be rejected.
 */
public class MockReviewer implements Reviewer {

  private static final Set<Long> BYTES_TO_REJECT = Sets.newHashSet();

  public static void resetBytesToReject(Set<Long> bytes) {
    BYTES_TO_REJECT.clear();
    BYTES_TO_REJECT.addAll(bytes);
  }

  @Override
  public boolean acceptAllocation(StorageDirView dirView) {
    long availableBytes = dirView.getAvailableBytes();
    return !BYTES_TO_REJECT.contains(availableBytes);
  }
}
