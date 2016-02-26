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

package alluxio.master.file.options;

import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a path.
 * @param <T> the type of the object to create
 */
@NotThreadSafe
public abstract class CreatePathOptions<T> {
  protected boolean mAllowExists;
  protected long mBlockSizeBytes;
  protected boolean mDirectory;
  protected boolean mMountPoint;
  protected long mOperationTimeMs;
  protected PermissionStatus mPermissionStatus;
  protected boolean mPersisted;
  protected boolean mRecursive;
  protected long mTtl;

  protected CreatePathOptions() throws IOException {
    mAllowExists = false;
    mBlockSizeBytes = MasterContext.getConf().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    mDirectory = false;
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mPermissionStatus = PermissionStatus.get(MasterContext.getConf(), true);
    mPersisted = false;
    mRecursive = false;
    mTtl = Constants.NO_TTL;
  }

  protected abstract T getThis();

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the directory flag; it specifies whether the object to create is a directory
   */
  public boolean isDirectory() {
    return mDirectory;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return the mount point flag; it specifies whether the object to create is a mount point
   */
  public boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * @return the permission status
   */
  public PermissionStatus getPermissionStatus() {
    return mPermissionStatus;
  }

  /**
   * @return the persisted flag; it specifies whether the object to create is persisted in UFS
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public T setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return getThis();
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public T setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @param mountPoint the mount point flag to use; it specifies whether the object to create is
   *        a mount point
   * @return the updated options object
   */
  public T setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return getThis();
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public T setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return getThis();
  }

  /**
   * @param permissionStatus the permission status to use
   * @return the updated options object
   */
  public T setPermissionStatus(PermissionStatus permissionStatus) {
    mPermissionStatus = permissionStatus;
    return getThis();
  }

  /**
   * @param persisted the persisted flag to use; it specifies whether the object to create is
   *        persisted in UFS
   * @return the updated options object
   */
  public T setPersisted(boolean persisted) {
    mPersisted = persisted;
    return getThis();
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public T setRecursive(boolean recursive) {
    mRecursive = recursive;
    return getThis();
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public T setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("allowExists", mAllowExists)
        .add("blockSizeBytes", mBlockSizeBytes).add("directory", mDirectory)
        .add("mountPoint", mMountPoint).add("operationTimeMs", mOperationTimeMs)
        .add("persisted", mPersisted).add("recursive", mRecursive).add("ttl", mTtl)
        .add("permissionStatus", mPermissionStatus).toString();
  }
}
