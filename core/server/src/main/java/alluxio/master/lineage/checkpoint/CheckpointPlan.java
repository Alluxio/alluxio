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

package alluxio.master.lineage.checkpoint;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A plan for checkpointing lineage. It essentially contains a list of lineages to checkpoint in
 * order.
 */
@ThreadSafe
public final class CheckpointPlan {
  /** A list of lineage ids to checkpoint. */
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
   * @return true if the checkpoint plan is empty, false otherwise
   */
  public boolean isEmpty() {
    return mToCheckPoint.isEmpty();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("toCheckPoint", mToCheckPoint).toString();
  }
}
