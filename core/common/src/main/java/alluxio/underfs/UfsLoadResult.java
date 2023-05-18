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

/**
 The UfsLoadResult represents the result of a load operation
 on an Under File System (UFS).
 It contains information about the loaded items, such as the count,
 whether it is truncated or not, and the continuation token.
 */
public class UfsLoadResult {

  private final Stream<UfsStatus> mItems;
  private final String mContinuationToken;
  private final boolean mIsTruncated;
  private final int mItemsCount;
  private final AlluxioURI mLastItem;
  private final boolean mFirstIsFile;
  private final boolean mIsObjectStore;

  /**
   * Constructs a new instance of {@link UfsLoadResult}.
   *
   * @param items the stream of loaded items
   * @param itemsCount the count of loaded items
   * @param continuationToken the continuation token for loading more items
   * @param lastItem the URI of the last item that was loaded
   * @param isTruncated whether the load operation was truncated due to reaching a limit
   * @param firstIsFile whether the first item in the stream is a file
   * @param isObjectStore whether the under file system is an object store
   */
  public UfsLoadResult(
      Stream<UfsStatus> items, int itemsCount, @Nullable String continuationToken,
      @Nullable AlluxioURI lastItem, boolean isTruncated, boolean firstIsFile,
      boolean isObjectStore) {
    mItems = items;
    mContinuationToken = continuationToken;
    mIsTruncated = isTruncated;
    mItemsCount = itemsCount;
    mLastItem = lastItem;
    mFirstIsFile = firstIsFile;
    mIsObjectStore = isObjectStore;
  }

  /**
   * @return true if the under file system is an object store, false otherwise
   */
  public boolean isIsObjectStore() {
    return mIsObjectStore;
  }

  /**
   * @return true if the first item in the stream is a file, false otherwise
   */
  public boolean isFirstFile() {
    return mFirstIsFile;
  }

  /**
   * @return an optional containing the URI of the last item that was loaded,
   * or empty if no items were loaded
   */
  public Optional<AlluxioURI> getLastItem() {
    return Optional.ofNullable(mLastItem);
  }

  /**
   * @return the count of loaded items
   */
  public int getItemsCount() {
    return mItemsCount;
  }

  /**
   * @return true if the load operation was truncated, false otherwise
   */
  public boolean isTruncated() {
    return mIsTruncated;
  }

  /**
   * @return the stream of loaded items
   */
  public Stream<UfsStatus> getItems() {
    return mItems;
  }

  /**
   * @return the continuation token for loading more items,
   * or null if there are no more items to load
   */
  public String getContinuationToken() {
    return mContinuationToken;
  }
}
