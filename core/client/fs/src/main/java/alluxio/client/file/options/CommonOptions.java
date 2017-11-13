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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.thrift.FileSystemMasterCommonTOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Common method options.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public abstract class CommonOptions<T> {
  private long mSyncInterval;

  /**
   * Creates a new instance with default values.
   */
  protected CommonOptions() {
    mSyncInterval = -1;
  }

  /**
   * @return the sync interval, in ms
   */
  public long getSyncInterval() {
    return mSyncInterval;
  }

  /**
   * @param syncInterval the sync interval, in ms
   * @return the updated options object
   */
  public T setSyncInterval(long syncInterval) {
    mSyncInterval = syncInterval;
    return getThis();
  }

  /**
   * @return {@code this} so that the abstract class can use the fluent builder pattern
   */
  protected abstract T getThis();

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

  public Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this)
        .add("syncInterval", mSyncInterval);
  }

  /**
   * @return Thrift representation of the options
   */
  public FileSystemMasterCommonTOptions commonThrift() {
    FileSystemMasterCommonTOptions options = new FileSystemMasterCommonTOptions();
    options.setSyncInterval(mSyncInterval);
    return options;
  }
}
