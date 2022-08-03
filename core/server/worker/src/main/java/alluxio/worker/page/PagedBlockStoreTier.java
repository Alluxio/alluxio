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

package alluxio.worker.page;

import alluxio.Constants;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

public class PagedBlockStoreTier implements StorageTier {
  public static final String DEFAULT_TIER =
      Configuration.getString(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL0_ALIAS);

  // TODO(bowen): revisit the assumption that this is always tier 0
  public static final int DEFAULT_TIER_ORDINAL = 0;
  public static final String DEFAULT_MEDIUM = Constants.MEDIUM_MEM;

  private ImmutableList<PagedBlockStoreDir> mDirs;
  private long mCapacityBytes;

  private PagedBlockStoreTier() {
    // instantiation through static factory methods
  }

  public static PagedBlockStoreTier create(AlluxioConfiguration conf) throws IOException {
    List<PageStoreDir> dirs = PageStoreDir.createPageStoreDirs(conf);
    PagedBlockStoreTier tier = new PagedBlockStoreTier();
    ImmutableList.Builder<PagedBlockStoreDir> builder = ImmutableList.builder();
    for (int i = 0; i < dirs.size(); i++) {
      builder.add(new PagedBlockStoreDir(dirs.get(i), tier, i));
    }
    ImmutableList<PagedBlockStoreDir> pagedBlockStoreDirs = builder.build();
    tier.setAllDirs(pagedBlockStoreDirs);
    return tier;
  }

  protected void setAllDirs(ImmutableList<PagedBlockStoreDir> dirs) {
    mDirs = dirs;
    mCapacityBytes = mDirs.stream().map(PagedBlockStoreDir::getCapacityBytes).reduce(0L, Long::sum);
  }

  @Override
  public int getTierOrdinal() {
    return DEFAULT_TIER_ORDINAL;
  }

  @Override
  public String getTierAlias() {
    return DEFAULT_TIER;
  }

  @Override
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  @Override
  public long getAvailableBytes() {
    return mDirs.stream().map(PagedBlockStoreDir::getAvailableBytes).reduce(0L, Long::sum);
  }

  @Nullable
  @Override
  public StorageDir getDir(int dirIndex) {
    if (dirIndex < 0 || dirIndex >= mDirs.size()) {
      return null;
    }
    return mDirs.get(dirIndex);
  }

  @Override
  public List<StorageDir> getStorageDirs() {
    // List<T> is invariant over T, so a generic function call is needed to make the types check
    // copyOf actually avoids a copy since mDirs is already ImmutableList.
    return ImmutableList.copyOf(mDirs);
  }

  public List<PageStoreDir> getPageStoreDirs() {
    return ImmutableList.copyOf(mDirs);
  }

  public List<PagedBlockStoreDir> getPagedBlockStoreDirs() {
    return mDirs;
  }

  @Override
  public List<String> getLostStorage() {
    return ImmutableList.of();
  }

  @Override
  public void removeStorageDir(StorageDir dir) {

  }
}
