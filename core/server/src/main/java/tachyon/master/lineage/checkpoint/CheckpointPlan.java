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

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * A plan for checkpointing lineage. It essentially contains a list of lineages to checkpoint in
 * order.
 */
@ThreadSafe
public final class CheckpointPlan {
  /** A list of lineage ids to checkpoint */
  private final List<Long> mToCheckPoint;

  /**
   * Creates a new instance of {@link CheckpointPlan}.
   *
   * @param toCheckPoint lineage ids to checkpoint
   */
  public CheckpointPlan(List<Long> toCheckPoint) {
    mToCheckPoint = Preconditions.checkNotNull(toCheckPoint);
  }

  /**
   * @return a list of lineages to check point in sequence
   */
  public List<Long> getLineagesToCheckpoint() {
    return mToCheckPoint;
  }

  /**
   * @return true if the chekcpoint plan is empty, false otherwise
   */
  public boolean isEmpty() {
    return mToCheckPoint.isEmpty();
  }

  @Override
  public String toString() {
    if (mToCheckPoint.isEmpty()) {
      return "check point plan is empty";
    }
    return "toCheckpoint: " + Joiner.on(", ").join(mToCheckPoint);
  }
}
