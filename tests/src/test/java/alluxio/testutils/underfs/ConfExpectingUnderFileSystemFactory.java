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

package alluxio.testutils.underfs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.local.LocalUnderFileSystem;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * A factory to create a {@link LocalUnderFileSystem} that requires expected conf, or throws
 * exceptions during creation. On success, a local under file system is created.
 */
public final class ConfExpectingUnderFileSystemFactory implements UnderFileSystemFactory {
  /** The scheme of this UFS. */
  public final String mScheme;
  /** The conf expected to create the ufs. */
  private final Map<String, String> mExpectedConf;

  public ConfExpectingUnderFileSystemFactory(String scheme, Map<String, String> expectedConf) {
    mScheme = scheme;
    mExpectedConf = expectedConf;
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkArgument(mExpectedConf.equals(conf.getMountSpecificConf()),
        "ufs conf {} does not match expected {}", conf, mExpectedConf);
    return new LocalUnderFileSystem(new AlluxioURI(new AlluxioURI(path).getPath()), conf);
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(mScheme + ":///");
  }
}
