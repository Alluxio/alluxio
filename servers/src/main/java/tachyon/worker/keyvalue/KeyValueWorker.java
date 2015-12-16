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

package tachyon.worker.keyvalue;

import com.google.common.base.Preconditions;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.WorkerBase;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockDataManager;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * A worker serving KeyValue queries
 */
public class KeyValueWorker extends WorkerBase {
  /** Configuration object */
  private final TachyonConf mTachyonConf;
  /** Block data manager for access block info */
  private final BlockDataManager mBlockDataManager;

  /**
   * Constructor of KeyValueWorker
   *
   * @param blockDataManager
   * @throws IOException
   */
  public KeyValueWorker(BlockDataManager blockDataManager) throws IOException {
    // TODO(apc999): make heartbeat thread count configurable
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("keyvalue-worker-heartbeat-%d", true)));
    mTachyonConf = WorkerContext.getConf();
    mBlockDataManager = Preconditions.checkNotNull(blockDataManager);
  }

  public void start() throws IOException {

  }

  public void stop() throws IOException {

  }
}
