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

package alluxio.worker.block;

/**
 * Utils related to block lock ID.
 */
public final class BlockLockIdUtil {
  /** The lock ID that indicates that a UFS block access token is acquired. */
  public static final long UFS_BLOCK_LOCK_ID = -1;
  /** The lock ID that indicates that a UFS block access token is not acquired. */
  public static final long UFS_BLOCK_READ_TOKEN_UNAVAILABLE = -2;

  /** This can be anything that is less than 0 except -1 or -2. */
  public static final long INVALID_LOCK_ID = -3;

  /**
   * @param lockId the lock ID
   * @return true if the lock ID is {@link BlockLockIdUtil#UFS_BLOCK_LOCK_ID}
   */
  public static boolean isUfsBlockLockId(long lockId) {
    return lockId == UFS_BLOCK_LOCK_ID;
  }

  /**
   * @param lockId the lock ID
   * @return true if the lock ID is {@link BlockLockIdUtil#UFS_BLOCK_READ_TOKEN_UNAVAILABLE}
   */
  public static boolean isUfsBlockReadTokenUnavailable(long lockId) {
    return lockId ==  UFS_BLOCK_READ_TOKEN_UNAVAILABLE;
  }

  /**
   * @param lockId the lock ID
   * @return true if the lock ID is a valid Alluxio lock ID
   */
  public static boolean isAlluxioBlockLockId(long lockId) {
    return lockId >= 0;
  }

  private BlockLockIdUtil() {}  // prevent instantiation
}
