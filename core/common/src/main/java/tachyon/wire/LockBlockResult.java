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

package tachyon.wire;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The lock block operation result.
 */
@NotThreadSafe
public final class LockBlockResult {
  private long mLockId;
  private String mBlockPath;

  /**
   * Creates a new instance of {@link LockBlockResult}.
   */
  public LockBlockResult() {
    mBlockPath = "";
  }

  /**
   * @return the lock id
   */
  public long getLockId() {
    return mLockId;
  }

  /**
   * @return the block path
   */
  public String getBlockPath() {
    return mBlockPath;
  }

  /**
   * @param lockId the lock id to use
   * @return the lock block operation result
   */
  public LockBlockResult setLockId(long lockId) {
    mLockId = lockId;
    return this;
  }

  /**
   * @param blockPath the block path to use
   * @return the lock block operation result
   */
  public LockBlockResult setBlockPath(String blockPath) {
    Preconditions.checkNotNull(blockPath);
    mBlockPath = blockPath;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LockBlockResult)) {
      return false;
    }
    LockBlockResult that = (LockBlockResult) o;
    return mLockId == that.mLockId && mBlockPath.equals(that.mBlockPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLockId, mBlockPath);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("lockId", mLockId).add("blockPath", mBlockPath)
        .toString();
  }
}
