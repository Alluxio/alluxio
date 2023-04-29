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

package alluxio.master.file.contexts;

import alluxio.conf.Configuration;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.util.FileSystemOptionsUtils;

import com.google.common.base.MoreObjects;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Used to merge and wrap {@link ListStatusPOptions}.
 */
public class ListStatusContext
    extends OperationContext<ListStatusPOptions.Builder, ListStatusContext> {

  private int mListedCount = 0;
  private int mProcessedCount = 0;
  private boolean mTruncated = false;
  private boolean mDoneListing = false;
  private long mTotalListings;
  private final ListStatusPartialPOptions.Builder mPartialPOptions;
  private List<String> mPartialListingEndingPath;

  /**
   *
   * @return the partial listing options
   */
  public Optional<ListStatusPartialPOptions.Builder> getPartialOptions() {
    return Optional.ofNullable(mPartialPOptions);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private ListStatusContext(ListStatusPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mPartialPOptions = null;
    mPartialListingEndingPath = Collections.emptyList();
  }

  /**
   * Creates context with given option data.
   *
   * @param partialOptionsBuilder options builder
   */
  private ListStatusContext(ListStatusPartialPOptions.Builder partialOptionsBuilder) {
    super(partialOptionsBuilder.getOptions().toBuilder());
    mPartialPOptions = partialOptionsBuilder;
    mPartialListingEndingPath = Collections.emptyList();
  }

  /**
   * Set the total number of listings in this call,
   * this should be -1 if a recursive listing.
   * @param count the number of listings
   */
  public void setTotalListings(long count) {
    mTotalListings = count;
  }

  /**
   * Get the value set by setTotalListing.
   * @return the number of listings
   */
  public long getTotalListings() {
    return mTotalListings;
  }

  /**
   * @param partialListingEndingPath the partial listing ending path the listing stops at
   */
  public void setPartialListingEndingPath(List<String> partialListingEndingPath) {
    mPartialListingEndingPath = partialListingEndingPath;
  }

  /**
   * @param optionsBuilder Builder for proto {@link ListStatusPOptions}
   * @return the instance of {@link ListStatusContext} with the given options
   */
  public static ListStatusContext create(ListStatusPOptions.Builder optionsBuilder) {
    return new ListStatusContext(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link ListStatusPOptions}
   * @return the instance of {@link ListStatusContext} with the given options
   */
  public static ListStatusContext create(ListStatusPartialPOptions.Builder optionsBuilder) {
    return new ListStatusContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link ListStatusPOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link ListStatusPOptions} to merge with defaults
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext mergeFrom(ListStatusPOptions.Builder optionsBuilder) {
    ListStatusPOptions masterOptions =
        FileSystemOptionsUtils.listStatusDefaults(Configuration.global());
    ListStatusPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * Merges and embeds the given {@link ListStatusPartialPOptions} with the corresponding
   * master options.
   *
   * @param optionsBuilder Builder for proto {@link ListStatusPartialPOptions} to merge with
   *                       defaults
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext mergeFrom(ListStatusPartialPOptions.Builder optionsBuilder) {
    return create(
        FileSystemOptionsUtils.listStatusPartialDefaults(
            Configuration.global()).toBuilder().mergeFrom(optionsBuilder.build()));
  }

  /**
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext defaults() {
    return create(FileSystemOptionsUtils.listStatusDefaults(Configuration.global()).toBuilder());
  }

  /**
   * Called each time an item is listed.
   * @param depth the recursion depth
   * @param inodeName the name of the inode
   * @return true if the item should be listed, false otherwise
   */
  public boolean listedItem(int depth, String inodeName) {
    if (mPartialPOptions != null) {
      mProcessedCount++;
      if (mPartialPOptions.getOffsetCount() >= mProcessedCount) {
        return false;
      }
      mListedCount++;
      if (mPartialPOptions.hasBatchSize()
          && mPartialPOptions.getBatchSize() < mListedCount) {
        mTruncated = true;
        mDoneListing = true;
        return false;
      }
      if (depth > 0 && mPartialListingEndingPath.size() >= depth
          && inodeName.compareTo(mPartialListingEndingPath.get(depth - 1)) > 0) {
        mDoneListing = true;
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if the listing has completed and no new items need to be processed
   */
  public boolean isDoneListing() {
    return mDoneListing;
  }

  /**
   * @return true if this call is a partial listing of files (either has StartAfter
   * set, has an offset set, or has a batch size set).
   */
  public boolean isPartialListing() {
    return mPartialPOptions != null;
  }

  /**
   *
   * @return true if this is a partial listing and at least the batch size elements have
   * been listed, false otherwise
   */
  public boolean donePartialListing() {
    return mTruncated;
  }

  /**
   * @return true if a partial listing and the result was truncated
   */
  public boolean isTruncated() {
    return mTruncated;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .toString();
  }
}
