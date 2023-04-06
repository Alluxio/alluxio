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

package alluxio.underfs;

import alluxio.AlluxioURI;

import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class UfsLoadResult {

  private final Stream<UfsStatus> mItems;
  private final String mContinuationToken;
  private final boolean mIsTruncated;
  private final int mItemsCount;
  private final AlluxioURI mLastItem;
  private final boolean mFirstIsfile;

  public UfsLoadResult(
      Stream<UfsStatus> items, int itemsCount, @Nullable String continuationToken,
      @Nullable AlluxioURI lastItem, boolean isTruncated, boolean firstIsFile) {
    mItems = items;
    mContinuationToken = continuationToken;
    mIsTruncated = isTruncated;
    mItemsCount = itemsCount;
    mLastItem = lastItem;
    mFirstIsfile = firstIsFile;
  }

  public boolean isFirstFile() {
    return mFirstIsfile;
  }

  public Optional<AlluxioURI> getLastItem() {
    return Optional.ofNullable(mLastItem);
  }

  public int getItemsCount() {
    return mItemsCount;
  }

  public boolean isTruncated() {
    return mIsTruncated;
  }

  public Stream<UfsStatus> getItems() {
    return mItems;
  }

  public String getContinuationToken() {
    return mContinuationToken;
  }
}
