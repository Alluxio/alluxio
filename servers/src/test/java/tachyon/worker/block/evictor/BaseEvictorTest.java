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

import java.io.File;
import java.util.Collections;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.TieredBlockStoreTestUtils;
import tachyon.worker.block.meta.StorageDir;

public class BaseEvictorTest {
  protected static final int USER_ID = 2;
  protected static final long BLOCK_ID = 10;

  protected BlockMetadataManager mMetaManager;
  protected BlockMetadataManagerView mManagerView;
  protected Evictor mEvictor;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  protected void cache(long userId, long blockId, long bytes, int tierLevel, int dirIdx)
      throws Exception {
    StorageDir dir = mMetaManager.getTiers().get(tierLevel).getDir(dirIdx);
    TieredBlockStoreTestUtils.cache(userId, blockId, bytes, dir, mMetaManager, mEvictor);
  }

  protected void init(String evictorClassName) throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mManagerView =
        new BlockMetadataManagerView(mMetaManager, Collections.<Integer>emptySet(),
            Collections.<Long>emptySet());
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_EVICT_STRATEGY_CLASS, evictorClassName);
    mEvictor = Evictor.Factory.createEvictor(conf, mManagerView);
  }
}
