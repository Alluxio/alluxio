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

package tachyon.master.lineage.checkpoint;

import com.google.common.base.Preconditions;

import tachyon.master.lineage.meta.LineageStore;
import tachyon.master.lineage.meta.LineageStoreView;

/**
 * Manages checkpointing.
 */
public final class CheckpointManager {
  private final LineageStore mLineageStore;

  public CheckpointManager(LineageStore lineageStore) {
    mLineageStore = Preconditions.checkNotNull(lineageStore);
  }

  public LineageStoreView getLineageStoreView() {
    return new LineageStoreView(mLineageStore);
  }

  public void acceptPlan(CheckpointPlan plan) {
    // TODO(yupeng): validation
  }
}
