/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.lineage.recompute;

import alluxio.master.lineage.meta.Lineage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A plan for recomputing the lost files. It essentially contains a batch of jobs from the
 * corresponding lineages to execute.
 *
 * TODO(yupeng): in this version it simply returns a list of jobs to execute in sequence. In the
 * future, we will explore the possibility of executing jobs in in parallel.
 */
@ThreadSafe
public class RecomputePlan {
  /** A list of lineages to recompute. */
  private final List<Lineage> mToRecompute;

  /**
   * Creates a new instance of {@link RecomputePlan}.
   *
   * @param toRecompute the lineages to recompute
   */
  public RecomputePlan(List<Lineage> toRecompute) {
    mToRecompute = Preconditions.checkNotNull(toRecompute);
  }

  /**
   * @return a list of lineages to recompute
   */
  public List<Lineage> getLineageToRecompute() {
    return mToRecompute;
  }

  /**
   * @return true if the plan is empty, false otherwise
   */
  public boolean isEmpty() {
    return mToRecompute.isEmpty();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("toRecompute", mToRecompute).toString();
  }
}
