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
  private LockBlockStatus mLockBlockStatus;

  /**
   * The status indicates the type of the lock acquired.
   */
  public enum LockBlockStatus {
    /**
     * A lock acquired for an Alluxio block.
     */
    ALLUXIO_BLOCK_LOCKED(1),
    /**
     * A lock acquired for a UFS block.
     */
    UFS_TOKEN_ACQUIRED(2),
    /**
     * Not lock acquired because the block is not in Alluxio and the UFS block is busy.
     */
    UFS_TOKEN_NOT_ACQUIRED(3),
    ;

    private final int mValue;

    LockBlockStatus(int value) {
      mValue = value;
    }

    /**
     * @return the lock status value
     */
    public int getValue() {
      return mValue;
    }

    /**
     * @return true if the block is in Alluxio
     */
    public boolean blockInAlluxio() {
      return mValue == ALLUXIO_BLOCK_LOCKED.mValue;
    }

    /**
     * @return true if a UFS block token is acquired
     */
    public boolean ufsTokenAcquired() {
      return mValue == UFS_TOKEN_ACQUIRED.mValue;
    }

    /**
     * @return true if a UFS token is not available
     */
    public boolean ufsTokenNotAcquired() {
      return mValue == UFS_TOKEN_NOT_ACQUIRED.mValue;
    }

    /**
     * @return the thrift version of the lock status
     */
    public alluxio.thrift.LockBlockStatus toThrift() {
      return alluxio.thrift.LockBlockStatus.findByValue(mValue);
    }

    /**
     * @param status the lock block status in thrift
     * @return the lock block status
     */
    public static LockBlockStatus fromThrift(alluxio.thrift.LockBlockStatus status) {
      switch (status) {
        case ALLUXIO_BLOCK_LOCKED:
          return LockBlockStatus.ALLUXIO_BLOCK_LOCKED;
        case UFS_TOKEN_ACQUIRED:
          return LockBlockStatus.UFS_TOKEN_ACQUIRED;
        case UFS_TOKEN_NOT_ACQUIRED:
          return LockBlockStatus.UFS_TOKEN_NOT_ACQUIRED;
        default:
          // Should never happen.
          throw new IllegalStateException("Unexpected lock block status.");
      }
    }
  }

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
    mLockBlockStatus = LockBlockStatus
        .fromThrift(Preconditions.checkNotNull(lockBlockResult.getLockBlockStatus()));
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
   * @return the lock block status
   */
  public LockBlockStatus getLockBlockStatus() {
    return mLockBlockStatus;
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
   * @param status the lock block status
   * @return the lock block operation result
   */
  public LockBlockResult setLockBlockStatus(LockBlockStatus status) {
    mLockBlockStatus = Preconditions.checkNotNull(status);
    return  this;
  }

  /**
   * @return thrift representation of the lock block operation result
   */
  protected alluxio.thrift.LockBlockResult toThrift() {
    return new alluxio.thrift.LockBlockResult(mLockId, mBlockPath, mLockBlockStatus.toThrift());
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
    return Objects.equal(mLockId, that.mLockId) && Objects.equal(mBlockPath, that.mBlockPath)
        && Objects.equal(mLockBlockStatus, that.mLockBlockStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLockId, mBlockPath, mLockBlockStatus);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("blockPath", mBlockPath)
        .add("lockBlockStatus", mLockBlockStatus).add("lockId", mLockId).toString();
  }
}
