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
 * This class is a wrapper of {@link StorageDir} to provided more limited access
 * and a filtered list of blocks.
 */
public class StorageDirView {

  private final StorageDir mDir;
  private final StorageTierView mTier;
  private final BlockMetadataView mView;

  public StorageDirView(StorageDir dir, StorageTierView tier, BlockMetadataView view) {
    mDir = Preconditions.checkNotNull(dir);
    mTier = tier;
    mView = Preconditions.checkNotNull(view);
  }

  public int getDirIndex() {
    return mDir.getDirIndex();
  }

  public List<BlockMeta> getBlocks() {
    // TODO: filter out blocks
    return mDir.getBlocks();
  }

  public long getAvailableBytes() {
    return mDir.getAvailableBytes();
  }

  public long getCommittedBytes() {
    // TODO: filter out bytes that are caused by blocks in pinlists, etc
    return mDir.getCommittedBytes();
  }

  public StorageDir getDirForCreatingBlock() {
    return mDir;
  }

  public StorageTierView getParentTier() {
    return mTier;
  }
}
