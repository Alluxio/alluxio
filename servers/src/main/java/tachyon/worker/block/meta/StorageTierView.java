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

package tachyon.worker.block.meta;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.StorageDirId;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataView;

/**
 * This class is a wrapper of {@link StorageTier} to provided more limited access
 */
public class StorageTierView {

  private final StorageTier mTier;
  private List<StorageDirView> mDirViews;
  private final BlockMetadataView mView;

  public StorageTierView(StorageTier tier, BlockMetadataView view) {
    mTier = Preconditions.checkNotNull(tier);
    mView = Preconditions.checkNotNull(view);

    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirView dirView = new StorageDirView(dir, this, view);
      mDirViews.add(dirView);
    }
  }

  public List<StorageDirView> getDirViews() {
    return mDirViews;
  }

  public StorageDirView getDirView(int dirIndex) throws IOException {
    return mDirViews.get(dirIndex);
  }

  public int getTierViewAlias() {
    return mTier.getTierAlias();
  }

  public int getTierViewLevel() {
    return mTier.getTierLevel();
  }
}
