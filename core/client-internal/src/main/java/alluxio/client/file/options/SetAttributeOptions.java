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

package alluxio.client.file.options;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.exception.PreconditionMessage;
import alluxio.thrift.SetAttributeTOptions;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting any number of a path's attributes. If a value is set as null, it
 * will be interpreted as an unset value and the current value will be unchanged.
 */
@PublicApi
@NotThreadSafe
public final class SetAttributeOptions {
  private Boolean mPinned;
  private Long mTtl;
  private Boolean mPersisted;
  private String mOwner;
  private String mGroup;
  private Short mPermission;
  private boolean mRecursive;

  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  private SetAttributeOptions() {
    mPinned = null;
    mTtl = null;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mPermission = Constants.INVALID_PERMISSION;
    mRecursive = false;
  }

  /**
   * @return true if the pinned flag is set, otherwise false
   */
  public boolean hasPinned() {
    return mPinned != null;
  }

  /**
   * @return the pinned flag value; it specifies whether the object should be kept in memory
   */
  public boolean getPinned() {
    Preconditions.checkState(hasPinned(), PreconditionMessage.MUST_SET_PINNED);
    return mPinned;
  }

  /**
   * @return true if the TTL value is set, otherwise false
   */
  public boolean hasTtl() {
    return mTtl != null;
  }

  /**
   * @return the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *         created file should be kept around before it is automatically deleted, irrespective of
   *         whether the file is pinned
   */
  public long getTtl() {
    Preconditions.checkState(hasTtl(), PreconditionMessage.MUST_SET_TTL);
    return mTtl;
  }

  /**
   * @return true if the persisted value is set, otherwise false
   */
  public boolean hasPersisted() {
    return mPersisted != null;
  }

  /**
   * @return the persisted value of the file; it denotes whether the file has been persisted to the
   *         under file system or not.
   */
  public boolean getPersisted() {
    Preconditions.checkState(hasPersisted(), PreconditionMessage.MUST_SET_PERSISTED);
    return mPersisted;
  }

  /**
   * @return true if the owner value is set, otherwise false
   */
  public boolean hasOwner() {
    return mOwner != null;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    Preconditions.checkState(hasOwner(), PreconditionMessage.MUST_SET_OWNER);
    return mOwner;
  }

  /**
   * @return true if the group value is set, otherwise false
   */
  public boolean hasGroup() {
    return mGroup != null;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    Preconditions.checkState(hasGroup(), PreconditionMessage.MUST_SET_GROUP);
    return mGroup;
  }

  /**
   * @return true if the permission value is set, otherwise false
   */
  public boolean hasPermission() {
    return mPermission != Constants.INVALID_PERMISSION;
  }

  /**
   * @return the permission
   */
  public short getPermission() {
    Preconditions.checkState(hasPermission(), PreconditionMessage.MUST_SET_PERMISSION);
    return mPermission;
  }

  /**
   * @return the recursive flag value
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param pinned the pinned flag value to use; it specifies whether the object should be kept in
   *        memory, if ttl(time to live) is set, the file will be deleted after expiration no
   *        matter this value is true or false
   * @return the updated options object
   */
  public SetAttributeOptions setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned
   * @return the updated options object
   */
  public SetAttributeOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param persisted the persisted flag value to use; it specifies whether the file has been
   *        persisted in the under file system or not.
   * @return the updated options object
   */
  public SetAttributeOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param owner to be set as the owner of a path
   * @return the updated options object
   */
  public SetAttributeOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @param group to be set as the group of a path
   * @return the updated options object
   */
  public SetAttributeOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param permission to be set as the permission of a path
   * @return the updated options object
   */
  public SetAttributeOptions setPermission(short permission) {
    mPermission = permission;
    return this;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive whether to set acl recursively under a directory
   * @return the updated options object
   */
  public SetAttributeOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @return Thrift representation of the options
   */
  public SetAttributeTOptions toThrift() {
    SetAttributeTOptions options = new SetAttributeTOptions();
    if (mPinned != null) {
      options.setPinned(mPinned);
    }
    if (mTtl != null) {
      options.setTtl(mTtl);
    }
    if (mPersisted != null) {
      options.setPersisted(mPersisted);
    }
    if (mOwner != null) {
      options.setOwner(mOwner);
    }
    if (mGroup != null) {
      options.setGroup(mGroup);
    }
    if (mPermission != Constants.INVALID_PERMISSION) {
      options.setPermission(mPermission);
    }
    options.setRecursive(mRecursive);
    return options;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SetStateOptions(");
    sb.append(super.toString()).append(", Pinned: ").append(mPinned).append(", TTL: ").append(mTtl)
        .append(", Persisted: ").append(mPersisted)
        .append(", Owner: ").append(mOwner)
        .append(", Group: ").append(mGroup)
        .append(", Permission: ").append(mPermission)
        .append(", Recursive: ").append(mRecursive);
    sb.append(")");
    return sb.toString();
  }
}
