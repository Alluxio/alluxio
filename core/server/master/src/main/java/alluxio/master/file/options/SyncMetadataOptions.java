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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for syncing the metadata of a path.
 */
@NotThreadSafe
public final class SyncMetadataOptions
    extends alluxio.file.options.SyncMetadataOptions<SyncMetadataOptions> {
  /**
   * @return the default {@link SyncMetadataOptions}
   */
  public static SyncMetadataOptions defaults() {
    return new SyncMetadataOptions();
  }

  /**
   * Constructs an instance of {@link SyncMetadataOptions} from {@link SyncMetadataTOptions}.
   *
   * @param options the {@link SyncMetadataTOptions} to use
   */
//  public SyncMetadataOptions(SyncMetadataTOptions options) {
//    this();
//    if (options != null) {
//      if (options.isSetCommonOptions()) {
//        mCommonOptions = new CommonOptions(options.getCommonOptions());
//      }
//    }
//  }

  /**
   * Constructs an instance from {@link CommonOptions}.
   *
   * @param options the {@link CommonOptions} to create from
   */
  public SyncMetadataOptions(CommonOptions options) {
    this();
    if (options != null) {
      mCommonOptions = new CommonOptions(options);
    }
  }

  private SyncMetadataOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
  }
}
