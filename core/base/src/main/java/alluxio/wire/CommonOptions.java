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

import alluxio.annotation.PublicApi;

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

  /**
   * @return the default {@link CommonOptions}
   */
  public static CommonOptions defaults() {
    return new CommonOptions();
  }

  protected CommonOptions() {
//    mSyncIntervalMs = Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL);
  }

//  /**
//   * Creates a new instance of {@link CommonOptions} from {@link FileSystemMasterCommonPOptions}.
//   *
//   * @param options Thrift options
//   */
//  public CommonOptions(FileSystemMasterCommonPOptions options) {
//    this();
//    if (options != null) {
//      if (options.hasSyncIntervalMs()) {
//        mSyncIntervalMs = options.getSyncIntervalMs();
//      }
//    }
//  }

  /**
   * Copies from {@link CommonOptions}.
   *
   * @param options the other option
   */
  public CommonOptions(CommonOptions options) {
    this();
    if (options != null) {
      mSyncIntervalMs = options.mSyncIntervalMs;
    }
  }

  /**
   * @return the sync interval, in milliseconds
   */
  public long getSyncIntervalMs() {
    return mSyncIntervalMs;
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
    return Objects.equal(mSyncIntervalMs, that.mSyncIntervalMs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSyncIntervalMs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("syncIntervalMs", mSyncIntervalMs)
        .toString();
  }

//  /**
//   * @return proto representation of common options
//   */
//  public FileSystemMasterCommonPOptions toProto() {
//    return  FileSystemMasterCommonPOptions.newBuilder()
//        .setSyncIntervalMs(mSyncIntervalMs)
//        .build();
//  }
}
