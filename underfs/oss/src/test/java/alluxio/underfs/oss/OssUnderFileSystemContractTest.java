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

package alluxio.underfs.oss;

import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Preconditions;

/**
 * This UFS contract test will use Swift as the backing store.
 */
public final class OSSUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String INTEGRATION_OSS_BUCKET = "ossBucket";

  private String mOSSBucket;

  public OSSUnderFileSystemContractTest() {
    String ossBucket = System.getProperty(INTEGRATION_OSS_BUCKET);
    Preconditions.checkState(ossBucket != null);
    Preconditions.checkState(new OSSUnderFileSystemFactory().supportsPath(ossBucket));
    mOSSBucket = ossBucket;
  }

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new OSSUnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    return mOSSBucket;
  }
}
