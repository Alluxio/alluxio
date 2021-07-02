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

package alluxio.worker.block;

import alluxio.exception.ExceptionMessage;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * This class is an abstract class for allocators and evictors to extend to provide
 * limited access to block metadata.
 */
public abstract class BlockMetadataView {
  /** The {@link BlockMetadataManager} this view is derived from. */
  protected final BlockMetadataManager mMetadataManager;
  protected final boolean mUseReservedSpace;

  /**
   * A list of {@link StorageTierView}, derived from {@link StorageTier}s from the
   * {@link BlockMetadataManager}.
   */
  final List<StorageTierView> mTierViews = new ArrayList<>();

  /** A map from tier alias to {@link StorageTierView}. */
  Map<String, StorageTierView> mAliasToTierViews = new HashMap<>();

  /**
   * Creates a new instance of {@link BlockMetadataView}.
   *
   * @param manager which the view should be constructed from
   */
  public BlockMetadataView(BlockMetadataManager manager) {
    this(manager, false);
  }

  /**
   * Creates a new instance of {@link BlockMetadataView}.
   *
   * @param manager which the view should be constructed from
   * @param useReservedSpace whether to include reserved space in available bytes
   */
  public BlockMetadataView(BlockMetadataManager manager, boolean useReservedSpace) {
    mMetadataManager = Preconditions.checkNotNull(manager, "manager");
    mUseReservedSpace = useReservedSpace;
    initializeView();
  }

  /**
   * Provides {@link StorageTierView} given tierAlias. Throws an {@link IllegalArgumentException} if
   * the tierAlias is not found.
   *
   * @param tierAlias the alias of this tierView
   * @return the {@link StorageTierView} object associated with the alias
   */
  public StorageTierView getTierView(String tierAlias) {
    StorageTierView tierView = mAliasToTierViews.get(tierAlias);
    if (tierView == null) {
      throw new IllegalArgumentException(
          ExceptionMessage.TIER_VIEW_ALIAS_NOT_FOUND.getMessage(tierAlias));
    } else {
      return tierView;
    }
  }

  /**
   * Gets all tierViews under this storage metadata view.
   *
   * @return the list of {@link StorageTierView}s
   */
  public List<StorageTierView> getTierViews() {
    return Collections.unmodifiableList(mTierViews);
  }

  /**
   * Gets the next storage tier view.
   *
   * @param tierView the storage tier view
   * @return the next storage tier view, null if this is the last tier view
   */
  @Nullable
  public StorageTierView getNextTier(StorageTierView tierView) {
    int nextOrdinal = tierView.getTierViewOrdinal() + 1;
    if (nextOrdinal < mTierViews.size()) {
      return mTierViews.get(nextOrdinal);
    }
    return null;
  }

  /**
   * Gets all tierViews before certain tierView. Throws an {@link IllegalArgumentException} if the
   * tierAlias is not found.
   *
   * @param tierAlias the alias of a tierView
   * @return the list of {@link StorageTierView}
   */
  public List<StorageTierView> getTierViewsBelow(String tierAlias) {
    int ordinal = getTierView(tierAlias).getTierViewOrdinal();
    return mTierViews.subList(ordinal + 1, mTierViews.size());
  }

  /**
   * Used to initialize the view based on the current metadata status.
   */
  protected abstract void initializeView();

  /**
   * Used to refresh the view based on current metadata status.
   *
   * @return the refreshed view instance
   */
  public BlockMetadataView refreshView() {
    // Clear previous views.
    mTierViews.clear();
    mAliasToTierViews.clear();
    // Re-initialize the view.
    initializeView();
    return this;
  }
}
