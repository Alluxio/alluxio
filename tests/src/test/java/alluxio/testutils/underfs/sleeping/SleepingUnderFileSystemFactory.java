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

package alluxio.testutils.underfs.sleeping;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

/**
 * Factory for creating {@link SleepingUnderFileSystem}. Should only be used in tests.
 */
public class SleepingUnderFileSystemFactory implements UnderFileSystemFactory {
  private SleepingUnderFileSystemOptions mOptions;
  private SleepingUnderFileSystem mUfs;

  /**
   * Constructs a new {@link SleepingUnderFileSystemFactory}.
   *
   * @param options the options to associate with any created under file systems
   */
  public SleepingUnderFileSystemFactory(SleepingUnderFileSystemOptions options) {
    mOptions = options;
  }

  /**
   * Constructs a new {@link SleepingUnderFileSystemFactory} for a given
   * {@link SleepingUnderFileSystem} instance. This can be useful to inject a spied instance.
   *
   * @param ufs the UFS instance to return on creation
   */
  public SleepingUnderFileSystemFactory(SleepingUnderFileSystem ufs) {
    mUfs = ufs;
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    // Managed blocking should be enabled when using Sleeping UFS.
    conf.set(PropertyKey.MASTER_UFS_MANAGED_BLOCKING_ENABLED, true);
    if (mUfs == null) {
      Preconditions.checkArgument(path != null, "path may not be null");
      return new SleepingUnderFileSystem(new AlluxioURI(path), mOptions, conf);
    } else {
      return mUfs;
    }
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith("sleep:///");
  }
}
