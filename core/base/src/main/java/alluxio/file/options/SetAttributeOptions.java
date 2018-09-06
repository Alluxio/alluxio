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

import alluxio.security.authorization.Mode;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting the attributes.
 */
@NotThreadSafe
public class SetAttributeOptions {
  protected CommonOptions mCommonOptions;
  protected Boolean mPinned;
  protected Long mTtl;
  protected TtlAction mTtlAction;
  protected Boolean mPersisted;
  protected String mOwner;
  protected String mGroup;
  protected Mode mMode;
  protected boolean mRecursive;
  protected long mOperationTimeMs;
  protected String mUfsFingerprint;

  protected SetAttributeOptions() {}

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
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
    return mMode.toShort();
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
   * @return the ufs fingerprint
   */
  public String getUfsFingerprint() {
    return mUfsFingerprint;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return (T) this;
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setPinned(boolean pinned) {
    mPinned = pinned;
    return (T) this;
  }

  /**
   * @param ttl the time-to-live (in seconds) to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setTtl(long ttl) {
    mTtl = ttl;
    return (T) this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return (T) this;
  }

  /**
   * @param persisted the persisted flag value to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setPersisted(boolean persisted) {
    mPersisted = persisted;
    return (T) this;
  }

  /**
   * @param owner the owner to use
   * @return the updated options object
   * @throws IllegalArgumentException if the owner is set to empty
   */
  public <T extends SetAttributeOptions> T setOwner(String owner) {
    if (owner != null && owner.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set owner to empty.");
    }
    mOwner = owner;
    return (T) this;
  }

  /**
   * @param group the group to use
   * @return the updated options object
   * @throws IllegalArgumentException if the group is set to empty
   */
  public <T extends SetAttributeOptions> T setGroup(String group) {
    if (group != null && group.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set group to empty");
    }
    mGroup = group;
    return (T) this;
  }

  /**
   * @param mode the mode bits to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setMode(short mode) {
    return setMode(new Mode(mode));
  }

  /**
   * @param mode the mode bits to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setMode(Mode mode) {
    mMode = mode;
    return (T) this;
  }

  /**
   * @param recursive whether owner / group / mode should be updated recursively
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setRecursive(boolean recursive) {
    mRecursive = recursive;
    return (T) this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return (T) this;
  }

  /**
   * @param ufsFingerprint the ufs fingerprint
   * @return the updated options object
   */
  public <T extends SetAttributeOptions> T setUfsFingerprint(String ufsFingerprint) {
    mUfsFingerprint = ufsFingerprint;
    return (T) this;
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
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction)
        && Objects.equal(mPersisted, that.mPersisted)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mRecursive, that.mRecursive)
        && mOperationTimeMs == that.mOperationTimeMs
        && Objects.equal(mUfsFingerprint, that.mUfsFingerprint);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mPinned, mTtl, mTtlAction, mPersisted, mOwner, mGroup, mMode, mRecursive,
            mOperationTimeMs, mCommonOptions, mUfsFingerprint);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("pinned", mPinned)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("persisted", mPersisted)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .add("recursive", mRecursive)
        .add("operationTimeMs", mOperationTimeMs)
        .add("ufsFingerprint", mUfsFingerprint)
        .toString();
  }
}
