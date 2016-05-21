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
package alluxio.master.file.options;

import alluxio.security.authorization.PermissionStatus;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a path.
 *
 * @param <T> the type of the object to create
 */
@NotThreadSafe
public abstract class CreatePathOptions<T> {
  protected boolean mMountPoint;
  protected long mOperationTimeMs;
  protected PermissionStatus mPermissionStatus;
  protected boolean mPersisted;
  // TODO(peis): Rename this to mCreateAncestors.
  protected boolean mRecursive;
  protected boolean mMetadataLoad;

  protected CreatePathOptions() {
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mPermissionStatus = PermissionStatus.defaults();
    mPersisted = false;
    mRecursive = false;
    mMetadataLoad = false;
  }

  protected abstract T getThis();

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
   * @return the metadataLoad flag; if true, the create path is a result of a metadata load
   */
  public boolean isMetadataLoad() {
    return mMetadataLoad;
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
   * @param metadataLoad the flag value to use; if true, the create path is a result of a
   *                     metadata load
   * @return the updated options object
   */
  public T setMetadataLoad(boolean metadataLoad) {
    mMetadataLoad = metadataLoad;
    return getThis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreatePathOptions)) {
      return false;
    }
    CreatePathOptions<?> that = (CreatePathOptions<?>) o;
    return Objects.equal(mMountPoint, that.mMountPoint)
        && Objects.equal(mPermissionStatus, that.mPermissionStatus)
        && Objects.equal(mPersisted, that.mPersisted)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mMetadataLoad, that.mMetadataLoad);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMountPoint, mPermissionStatus, mPersisted, mRecursive, mMetadataLoad);
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this)
        .add("mountPoint", mMountPoint)
        .add("operationTimeMs", mOperationTimeMs)
        .add("permissionStatus", mPermissionStatus)
        .add("persisted", mPersisted)
        .add("recursive", mRecursive)
        .add("metadataLoad", mMetadataLoad);
  }
}
