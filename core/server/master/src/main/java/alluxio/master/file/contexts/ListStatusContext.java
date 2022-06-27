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
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link ListStatusPOptions}.
 */
public class ListStatusContext
    extends OperationContext<ListStatusPOptions.Builder, ListStatusContext> {

  /**
   *  The number of items listed so far.
   */
  private int mListedCount;
  private boolean mTruncated = false;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private ListStatusContext(ListStatusPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
  }

  /**
   * @param optionsBuilder Builder for proto {@link ListStatusPOptions}
   * @return the instance of {@link ListStatusContext} with the given options
   */
  public static ListStatusContext create(ListStatusPOptions.Builder optionsBuilder) {
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
        FileSystemOptions.listStatusDefaults(Configuration.global());
    ListStatusPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link ListStatusContext} with default values for master
   */
  public static ListStatusContext defaults() {
    return create(FileSystemOptions.listStatusDefaults(Configuration.global()).toBuilder());
  }

  /**
   * Called each time an item is listed.
   * @return true if the item should be listed, false otherwise
   * The last item is a partial listing is not listed and just used
   * to set the result as being truncated or not.
   */
  public boolean listedItem() {
    mListedCount++;
    if (getOptions().hasBatchSize() && getOptions().getBatchSize() < mListedCount) {
      mTruncated = true;
      return false;
    }
    return true;
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
