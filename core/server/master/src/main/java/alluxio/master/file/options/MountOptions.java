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

import alluxio.proto.journal.File;
import alluxio.thrift.MountTOptions;

import java.util.HashMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for mounting.
 */
@NotThreadSafe
public final class MountOptions extends alluxio.file.options.MountOptions {
  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static MountOptions defaults() {
    return new MountOptions();
  }

  private MountOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mReadOnly = false;
    mProperties = new HashMap<>();
    mShared = false;
  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link MountTOptions}.
   *
   * @param options Thrift options
   */
//  public MountOptions(MountTOptions options) {
//    this();
//    if (options != null) {
//      if (options.isSetCommonOptions()) {
//        mCommonOptions = new CommonOptions(options.getCommonOptions());
//      }
//      if (options.isSetReadOnly()) {
//        mReadOnly = options.isReadOnly();
//      }
//      if (options.isSetProperties()) {
//        mProperties.putAll(options.getProperties());
//      }
//      if (options.isShared()) {
//        mShared = options.isShared();
//      }
//    }
//  }

  /**
   * Creates a new instance of {@link MountOptions} from {@link File.AddMountPointEntry}.
   *
   * @param options Proto options
   */
  public MountOptions(File.AddMountPointEntry options) {
    this();
    if (options != null) {
      if (options.hasReadOnly()) {
        mReadOnly = options.getReadOnly();
      }
      for (File.StringPairEntry entry : options.getPropertiesList()) {
        mProperties.put(entry.getKey(), entry.getValue());
      }
      if (options.hasShared()) {
        mShared = options.getShared();
      }
    }
  }
}
