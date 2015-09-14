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

package tachyon.master.block.meta;

/**
 * The location of a Tachyon block.
 */
public final class MasterBlockLocation {
  /** The id of the Tachyon worker. */
  private final long mWorkerId;
  /** The tier that the block is on in this worker. */
  private int mTier;

  MasterBlockLocation(long workerId, int tier) {
    mWorkerId = workerId;
    mTier = tier;
  }

  /**
   * @return the Tachyon worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * @return the tier that the block is on in this worker.
   */
  public int getTier() {
    return mTier;
  }
}
