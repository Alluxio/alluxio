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

import alluxio.Constants;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting the attributes.
 */
@NotThreadSafe
public final class SetAttributeOptions {
  private Boolean mPinned;
  private Long mTtl;
  private TtlAction mTtlAction;
  private Boolean mPersisted;
  private String mOwner;
  private String mGroup;
  private Short mMode;
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
    mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
    mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
    mOwner = options.isSetOwner() ? options.getOwner() : null;
    mGroup = options.isSetGroup() ? options.getGroup() : null;
    mMode = options.isSetMode() ? options.getMode() : Constants.INVALID_MODE;
    mRecursive = options.isRecursive();
    mOperationTimeMs = System.currentTimeMillis();
  }

  private SetAttributeOptions() {
    mPinned = null;
    mTtl = null;
    mTtlAction = TtlAction.DELETE;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mMode = Constants.INVALID_MODE;
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
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
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
   * @return the mode bits
   */
  public Short getMode() {
    return mMode;
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
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public SetAttributeOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
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
   * @throws IllegalArgumentException if the owner is set to empty
   */
  public SetAttributeOptions setOwner(String owner) {
    if (owner != null && owner.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set owner to empty.");
    }
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to use
   * @return the updated options object
   * @throws IllegalArgumentException if the group is set to empty
   */
  public SetAttributeOptions setGroup(String group) {
    if (group != null && group.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set group to empty");
    }
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode bits to use
   * @return the updated options object
   */
  public SetAttributeOptions setMode(short mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param recursive whether owner / group / mode should be updated recursively
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SetAttributeOptions)) {
      return false;
    }
    SetAttributeOptions that = (SetAttributeOptions) o;
    return Objects.equal(mPinned, that.mPinned)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction)
        && Objects.equal(mPersisted, that.mPersisted)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPinned, mTtl, mTtlAction, mPersisted, mOwner, mGroup, mMode,
        mRecursive);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mPinned", mPinned)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("persisted", mPersisted)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .add("recursive", mRecursive)
        .add("operationTimeMs", mOperationTimeMs)
        .toString();
  }
}
