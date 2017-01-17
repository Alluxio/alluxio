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

package alluxio.underfs.sleepfs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import com.google.common.base.Preconditions;

/**
 * Factory for creating {@link SleepingUnderFileSystem}. Should only be used by tests. Modify
 * options by using the getter and setter to change behavior during a test.
 */
public class SleepingUnderFileSystemFactory implements UnderFileSystemFactory {
  private SleepingUnderFileSystemOptions mOptions;

  /**
   * Constructs a new {@link SleepingUnderFileSystemFactory}.
   */
  public SleepingUnderFileSystemFactory(SleepingUnderFileSystemOptions options) {
    mOptions = options;
  }

  /**
   * @return options of this sleeping ufs factory
   */
  public SleepingUnderFileSystemOptions getOptions() {
    return mOptions;
  }

  /**
   * @param options the options for this sleeping ufs factory
   */
  public void setOptions(SleepingUnderFileSystemOptions options) {
    mOptions = options;
  }

  @Override
  public UnderFileSystem create(String path, Object ufsConf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    return new SleepingUnderFileSystem(new AlluxioURI(path), mOptions);
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith("sleep:///");
  }
}
