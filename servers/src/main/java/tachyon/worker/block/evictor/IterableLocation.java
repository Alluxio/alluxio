/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.evictor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * Iterate over all StorageDir contained in BlockStoreLocation tier by tier.
 *
 * If the location is {@link tachyon.worker.block.BlockStoreLocation#anyTier()}, iterate over
 * {@link tachyon.worker.block.BlockMetadataManager#getTiers()}, in each tier, iterate over
 * {@link tachyon.worker.block.meta.StorageTier#getStorageDirs()}.
 *
 * If the location is {@link tachyon.worker.block.BlockStoreLocation#anyDirInTier(int)}, iterate
 * over {@link tachyon.worker.block.meta.StorageTier#getStorageDirs()}.
 *
 * If the location is a determined StorageDir, just visit itself once.
 */
class IterableLocation implements Iterable<StorageDir> {
  private final BlockMetadataManager mMeta;
  private final BlockStoreLocation mLocation;
  private TieredIterator mIterator;

  class TieredIterator implements Iterator<StorageDir> {
    private List<StorageTier> mTiers;
    private List<StorageDir> mDirs;
    private int mTierIdx;
    private int mDirIdx;

    public TieredIterator() throws IOException {
      mTierIdx = 0;
      mDirIdx = 0;

      if (mLocation.isAnyTier()) {
        mTiers = mMeta.getTiers();
      } else {
        mTiers = new ArrayList<StorageTier>(1);
        mTiers.add(mMeta.getTier(mLocation.tierAlias()));
      }

      if (mLocation.isDetermined()) {
        mDirs = new ArrayList<StorageDir>(1);
        mDirs.add(mTiers.get(0).getDir(mLocation.dir()));
      } else {
        mDirs = mTiers.get(0).getStorageDirs();
      }
    }

    @Override
    public boolean hasNext() {
      return mTierIdx < mTiers.size() && mDirIdx < mDirs.size();
    }

    @Override
    public StorageDir next() {
      StorageDir ret = mDirs.get(mDirIdx);
      mDirIdx ++;
      if (mDirIdx >= mDirs.size()) {
        mTierIdx ++;
        if (mTierIdx < mTiers.size()) {
          mDirs = mTiers.get(mTierIdx).getStorageDirs();
          mDirIdx = 0;
        }
      }
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private IterableLocation(BlockMetadataManager meta, BlockStoreLocation location)
      throws IOException {
    mMeta = meta;
    mLocation = location;
    mIterator = new TieredIterator();
  }

  public static IterableLocation create(BlockMetadataManager meta, BlockStoreLocation location)
      throws IOException {
    return new IterableLocation(meta, location);
  }

  @Override
  public Iterator<StorageDir> iterator() {
    return mIterator;
  }
}
