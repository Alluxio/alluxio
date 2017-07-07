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
import org.junit.BeforeClass;

/**
 * This UFS contract test will use Aliyun OSS as the backing store.
 */
public final class OSSUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String OSS_BUCKET_CONF = "testOSSBucket";
  private static final String OSS_BUCKET = System.getProperty(OSS_BUCKET_CONF);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Preconditions.checkNotNull(OSS_BUCKET);
    Preconditions.checkState(new OSSUnderFileSystemFactory().supportsPath(OSS_BUCKET),
        "%s is not a valid OSS path", OSS_BUCKET);
  }

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new OSSUnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    return OSS_BUCKET;
  }
}
