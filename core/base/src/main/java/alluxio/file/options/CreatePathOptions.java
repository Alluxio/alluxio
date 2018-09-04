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

package alluxio.file.options;

import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
<<<<<<< HEAD:core/base/src/main/java/alluxio/file/options/CreatePathOptions.java
||||||| merged common ancestors
import alluxio.wire.CommonOptions;
=======
import alluxio.wire.CommonOptions;
import alluxio.wire.TtlAction;
>>>>>>> master:core/server/master/src/main/java/alluxio/master/file/options/CreatePathOptions.java

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a path.
 *
 * @param <T> the type of the object to create
 */
@NotThreadSafe
public abstract class CreatePathOptions<T> {
  protected CommonOptions mCommonOptions;
  protected boolean mMountPoint;
  protected long mOperationTimeMs;
  protected String mOwner;
  protected String mGroup;
  protected Mode mMode;
  protected List<AclEntry> mAcl;
  protected boolean mPersisted;
  // TODO(peis): Rename this to mCreateAncestors.
  protected boolean mRecursive;
  protected boolean mMetadataLoad;

<<<<<<< HEAD:core/base/src/main/java/alluxio/file/options/CreatePathOptions.java
||||||| merged common ancestors
  protected CreatePathOptions() {
    mCommonOptions = CommonOptions.defaults();
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mOwner = "";
    mGroup = "";
    mMode = Mode.defaults();
    mPersisted = false;
    mRecursive = false;
    mMetadataLoad = false;
  }

=======
  protected CreatePathOptions() {
    mCommonOptions = CommonOptions.defaults();
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mOwner = "";
    mGroup = "";
    mMode = Mode.defaults();
    mAcl = Collections.emptyList();
    mPersisted = false;
    mRecursive = false;
    mMetadataLoad = false;
  }

>>>>>>> master:core/server/master/src/main/java/alluxio/master/file/options/CreatePathOptions.java
  protected abstract T getThis();

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
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
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the mode
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return an immutable list of ACL entries
   */
  public List<AclEntry> getAcl() {
    return mAcl;
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
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return getCommonOptions().getTtl();
  }

  /**
   * @return action {@link TtlAction} after ttl expired
   */
  public TtlAction getTtlAction() {
    return getCommonOptions().getTtlAction();
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public T setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
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
   * @param owner the owner to use
   * @return the updated options object
   */
  public T setOwner(String owner) {
    mOwner = owner;
    return getThis();
  }

  /**
   * @param group the group to use
   * @return the updated options object
   */
  public T setGroup(String group) {
    mGroup = group;
    return getThis();
  }

  /**
   * @param mode the mode to use
   * @return the updated options object
   */
  public T setMode(Mode mode) {
    mMode = mode;
    return getThis();
  }

  /**
   * Sets an immutable copy of acl as the internal access control list.
   *
   * @param acl the ACL entries
   * @return the updated options object
   */
  public T setAcl(List<AclEntry> acl) {
    mAcl = ImmutableList.copyOf(acl);
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

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public T setTtl(long ttl) {
    getCommonOptions().setTtl(ttl);
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction}; It informs the action to take when Ttl is expired;
   * @return the updated options object
   */
  public T setTtlAction(TtlAction ttlAction) {
    getCommonOptions().setTtlAction(ttlAction);
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
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mAcl, that.mAcl)
        && Objects.equal(mPersisted, that.mPersisted)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mMetadataLoad, that.mMetadataLoad)
        && mOperationTimeMs == that.mOperationTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mMountPoint, mOwner, mGroup, mMode, mAcl, mPersisted, mRecursive, mMetadataLoad,
            mOperationTimeMs, mCommonOptions);
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("mountPoint", mMountPoint)
        .add("operationTimeMs", mOperationTimeMs)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .add("acl", mAcl)
        .add("persisted", mPersisted)
        .add("recursive", mRecursive)
        .add("metadataLoad", mMetadataLoad);
  }
}
