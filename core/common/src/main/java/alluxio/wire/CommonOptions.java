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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.thrift.FileSystemMasterCommonTOptions;

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Common method options.
 */
@PublicApi
@NotThreadSafe
public final class CommonOptions implements Serializable {
  private static final long serialVersionUID = -1491370184123698287L;

  private long mSyncIntervalMs;
  private boolean mInheritParentPermissions;

  /**
   * @return the default {@link CommonOptions}
   */
  public static CommonOptions defaults() {
    return new CommonOptions();
  }

  protected CommonOptions() {
    mSyncIntervalMs = Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL);
    mInheritParentPermissions =
        Configuration.getBoolean(PropertyKey.USER_FILE_INHERIT_PARENT_PERMISSIONS);
  }

  /**
   * Creates a new instance of {@link CommonOptions} from {@link FileSystemMasterCommonTOptions}.
   *
   * @param options Thrift options
   */
  public CommonOptions(FileSystemMasterCommonTOptions options) {
    this();
    if (options != null) {
      if (options.isSetSyncIntervalMs()) {
        mSyncIntervalMs = options.getSyncIntervalMs();
      }
      if (options.isSetInheritParentPermissions()) {
        mInheritParentPermissions = options.isInheritParentPermissions();
      }
    }
  }

  /**
   * Copies from {@link CommonOptions}.
   *
   * @param options the other option
   */
  public CommonOptions(CommonOptions options) {
    this();
    if (options != null) {
      mSyncIntervalMs = options.mSyncIntervalMs;
      mInheritParentPermissions = options.mInheritParentPermissions;
    }
  }

  /**
   * @return the sync interval, in milliseconds
   */
  public long getSyncIntervalMs() {
    return mSyncIntervalMs;
  }

  /**
   * @return true if parent permissions should be inherited for created paths
   */
  public boolean isInheritParentPermissions() {
    return mInheritParentPermissions;
  }

  /**
   * @param inheritParentPermissions true if parent permissions should be inherited
   * @return the updated options object
   */
  public CommonOptions setInheritParentPermissions(boolean inheritParentPermissions) {
    mInheritParentPermissions = inheritParentPermissions;
    return this;
  }

  /**
   * @param syncIntervalMs the sync interval, in milliseconds
   * @return the updated options object
   */
  public CommonOptions setSyncIntervalMs(long syncIntervalMs) {
    mSyncIntervalMs = syncIntervalMs;
    return this;
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
        && Objects.equal(mInheritParentPermissions, that.mInheritParentPermissions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSyncIntervalMs, mInheritParentPermissions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("syncIntervalMs", mSyncIntervalMs)
        .add("inheritParentPermissions", mInheritParentPermissions)
        .toString();
  }

  /**
   * @return thrift representation of the lineage information
   */
  public FileSystemMasterCommonTOptions toThrift() {
    FileSystemMasterCommonTOptions options = new FileSystemMasterCommonTOptions();
    options.setSyncIntervalMs(mSyncIntervalMs);
    options.setInheritParentPermissions(mInheritParentPermissions);
    return options;
  }
}
