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

package alluxio.worker.block.meta;

import alluxio.exception.ExceptionMessage;
import alluxio.worker.block.BlockMetadataManager;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class exposes a narrower read-only view of storage metadata to Allocators.
 */
@NotThreadSafe
public class StorageMetadataView {

  /**
   * A list of {@link StorageTierView}, derived from {@link StorageTier}s from the
   * {@link BlockMetadataManager}.
   */
  private final List<StorageTierView> mTierViews = new ArrayList<>();

  /** A map from tier alias to {@link StorageTierView}. */
  private Map<String, StorageTierView> mAliasToTierViews = new HashMap<>();

  /**
   * Creates a new instance of {@link StorageMetadataView}.
   *
   * @param manager which the view should be constructed from
   */
  public StorageMetadataView(BlockMetadataManager manager) {
    Preconditions.checkNotNull(manager, "manager");

    for (StorageTier tier : manager.getTiers()) {
      StorageTierView tierView = new StorageTierView(tier);
      mTierViews.add(tierView);
      mAliasToTierViews.put(tier.getTierAlias(), tierView);
    }
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
}
