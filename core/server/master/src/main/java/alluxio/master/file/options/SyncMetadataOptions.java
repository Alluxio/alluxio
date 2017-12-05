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

import alluxio.thrift.SyncMetadataTOptions;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for syncing the metadata of a path.
 */
@NotThreadSafe
public final class SyncMetadataOptions {
  private CommonOptions mCommonOptions;

  /**
   * @return the default {@link SyncMetadataOptions}
   */
  public static SyncMetadataOptions defaults() {
    return new SyncMetadataOptions();
  }

  /**
   * Constructs an instance of {@link SyncMetadataOptions} from
   * {@link SyncMetadataTOptions}.
   *
   * @param options the {@link SyncMetadataTOptions} to use
   */
  public SyncMetadataOptions(SyncMetadataTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
    }
  }

  // TODO(gpang): unused?
  SyncMetadataOptions(CommonOptions options) {
    this();
    if (options != null) {
      mCommonOptions = CommonOptions.defaults().setSyncIntervalMs(options.getSyncIntervalMs());
    }
  }

  private SyncMetadataOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SyncMetadataOptions)) {
      return false;
    }
    SyncMetadataOptions that = (SyncMetadataOptions) o;
    return Objects.equal(mCommonOptions, that.mCommonOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .toString();
  }
}
