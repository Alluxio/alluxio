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

package alluxio.master.mdsync;

import alluxio.underfs.UfsStatus;

import java.util.stream.Stream;
import javax.annotation.Nullable;

class UfsLoadResult {

  private final Stream<UfsStatus> mItems;
  private final String mContinuationToken;
  private final boolean mIsTruncated;
  private final int mItemsCount;

  UfsLoadResult(
      Stream<UfsStatus> items, int itemsCount, @Nullable String continuationToken,
      boolean isTruncated) {
    mItems = items;
    mContinuationToken = continuationToken;
    mIsTruncated = isTruncated;
    mItemsCount = itemsCount;
  }

  int getItemsCount() {
    return mItemsCount;
  }

  boolean isTruncated() {
    return mIsTruncated;
  }

  Stream<UfsStatus> getItems() {
    return mItems;
  }

  String getContinuationToken() {
    return mContinuationToken;
  }
}
