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

import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Common method options.
 */
@NotThreadSafe
public class CommonOptions implements Serializable {
  private static final long serialVersionUID = -1491370184123698287L;

  protected long mSyncIntervalMs;

  /** Below ttl and ttl action are for loading files. */
  protected long mTtl;
  protected TtlAction mTtlAction;

  protected CommonOptions() {}

  /**
   * @return the sync interval, in milliseconds
   */
  public long getSyncIntervalMs() {
    return mSyncIntervalMs;
  }

  /**
   * @return the ttl for loaded files, in milliseconds
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return ttl action after ttl expired
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @param syncIntervalMs the sync interval, in milliseconds
   * @return the updated options object
   */
  public <T extends CommonOptions> T setSyncIntervalMs(long syncIntervalMs) {
    mSyncIntervalMs = syncIntervalMs;
    return (T) this;
  }

  /**
   * @param ttl time to live for files loaded by client, in milliseconds
   * @return the updated options object
   */
  public <T extends CommonOptions> T setTtl(long ttl) {
    mTtl = ttl;
    return (T) this;
  }

  /**
   * @param ttlAction action after ttl expired. DELETE by default
   * @return the updated options object
   */
  public <T extends CommonOptions> T setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return (T) this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommonOptions)) {
      return false;
    }
    CommonOptions that = (CommonOptions) o;
    return Objects.equal(mSyncIntervalMs, that.mSyncIntervalMs)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSyncIntervalMs, mTtl, mTtlAction);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("syncIntervalMs", mSyncIntervalMs)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .toString();
  }
}
