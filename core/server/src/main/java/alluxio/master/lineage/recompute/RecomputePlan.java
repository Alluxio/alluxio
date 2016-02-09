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

package alluxio.master.lineage.recompute;

import alluxio.master.lineage.meta.Lineage;

import com.google.common.base.Joiner;
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
    return "toRecompute: " + Joiner.on(", ").join(mToRecompute);
  }
}
