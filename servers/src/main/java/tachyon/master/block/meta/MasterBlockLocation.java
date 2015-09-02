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
 * The location of the block.
 */
public final class MasterBlockLocation {
  private final long mWorkerId;
  private int mTier;

  MasterBlockLocation(long workerId, int tier) {
    mWorkerId = workerId;
    mTier = tier;
  }

  public long getWorkerId() {
    return mWorkerId;
  }

  public int getTier() {
    return mTier;
  }

  public void setTier(int tier) {
    mTier = tier;
  }
}
