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

package alluxio.wire;

import alluxio.worker.block.BlockLockIdUtil;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lock block operation result.
 */
@NotThreadSafe
public final class LockBlockResult implements Serializable {
  private static final long serialVersionUID = -2217323649674837526L;

  private long mLockId;
  private String mBlockPath = "";

  /**
   * Creates a new instance of {@link LockBlockResult}.
   */
  public LockBlockResult() {}

  /**
   * Creates a new instance of {@link LockBlockResult} from a thrift representation.
   *
   * @param lockBlockResult the thrift representation of a lock block operation result
   */
  protected LockBlockResult(alluxio.thrift.LockBlockResult lockBlockResult) {
    mLockId = lockBlockResult.getLockId();
    mBlockPath = lockBlockResult.getBlockPath();
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

  /**
   * Checks whether the block is cached in Alluxio.
   *
   * This method is ignored by Jackson as its method name does not start with "is" or "get".
   *
   * @return true if the block is in cached in Alluxio
   */
  public boolean blockCachedInAlluxio() {
    return BlockLockIdUtil.isAlluxioBlockLockId(getLockId());
  }

  /**
   * @return thrift representation of the lock block operation result
   */
  protected alluxio.thrift.LockBlockResult toThrift() {
    return new alluxio.thrift.LockBlockResult(mLockId, mBlockPath);
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
