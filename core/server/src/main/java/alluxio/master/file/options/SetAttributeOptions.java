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
import alluxio.thrift.SetAttributeTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting the attributes.
 */
@NotThreadSafe
public class SetAttributeOptions {
  private Boolean mPinned;
  private Long mTtl;
  private Boolean mPersisted;
  private String mOwner;
  private String mGroup;
  private Short mPermission;
  private boolean mRecursive;
  private long mOperationTimeMs;

  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  /**
   * Constructs a new method option for setting the attributes.
   *
   * @param options the options for setting the attributes
   */
  public SetAttributeOptions(SetAttributeTOptions options) {
    mPinned = options.isSetPinned() ? options.isPinned() : null;
    mTtl = options.isSetTtl() ? options.getTtl() : null;
    mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
    mOwner = options.isSetOwner() ? options.getOwner() : null;
    mGroup = options.isSetGroup() ? options.getGroup() : null;
    mPermission =
        options.isSetPermission() ? options.getPermission() : Constants.INVALID_PERMISSION;
    mRecursive = options.isRecursive();
    mOperationTimeMs = System.currentTimeMillis();
  }

  private SetAttributeOptions() {
    mPinned = null;
    mTtl = null;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mPermission = Constants.INVALID_PERMISSION;
    mRecursive = false;
    mOperationTimeMs = System.currentTimeMillis();
  }

  /**
   * @return the pinned flag value
   */
  public Boolean getPinned() {
    return mPinned;
  }

  /**
   * @return the time-to-live (in seconds)
   */
  public Long getTtl() {
    return mTtl;
  }

  /**
   * @return the persisted flag value
   */
  public Boolean getPersisted() {
    return mPersisted;
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
   * @return the permission bits
   */
  public Short getPermission() {
    return mPermission;
  }

  /**
   * @return the recursive flag value
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the operation time (in milliseconds)
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the updated options object
   */
  public SetAttributeOptions setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param ttl the time-to-live (in seconds) to use
   * @return the updated options object
   */
  public SetAttributeOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param persisted the persisted flag value to use
   * @return the updated options object
   */
  public SetAttributeOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param owner the owner to use
   * @return the updated options object
   */
  public SetAttributeOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to use
   * @return the updated options object
   */
  public SetAttributeOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param permission the permission bits to use
   * @return the updated options object
   */
  public SetAttributeOptions setPermission(short permission) {
    mPermission = permission;
    return this;
  }

  /**
   * @param recursive whether owner / group / permission should be updated recursively
   * @return the updated options object
   */
  public SetAttributeOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public SetAttributeOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }
}
