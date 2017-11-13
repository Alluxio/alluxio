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

import alluxio.thrift.FileSystemMasterCommonTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Common options.
 */
@NotThreadSafe
public class CommonOptions {
  private long mSyncInterval;

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static CommonOptions defaults() {
    return new CommonOptions();
  }

  protected CommonOptions() {
    mSyncInterval = -1;
  }

  /**
   * Creates a new instance of {@link CommonOptions} from {@link FileSystemMasterCommonTOptions}.
   *
   * @param options Thrift options
   */
  public CommonOptions(FileSystemMasterCommonTOptions options) {
    this();
    if (options != null) {
      if (options.isSetSyncInterval()) {
        mSyncInterval = options.getSyncInterval();
      }
    }
  }

  /**
   * @return the sync interval, in milliseconds
   */
  public long getSyncInterval() {
    return mSyncInterval;
  }

  /**
   * @param syncInterval the sync interval, in milliseconds
   * @return the updated options object
   */
  public CommonOptions setSyncInterval(long syncInterval) {
    mSyncInterval = syncInterval;
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
    return Objects.equal(mSyncInterval, that.mSyncInterval);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSyncInterval);
  }

  @Override
  public String toString() {
    return toStringHelper().toString();
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this).add("syncInterval", mSyncInterval);
  }
}
