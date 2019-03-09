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

package alluxio.underfs.bos;

import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Preconditions;
import org.junit.BeforeClass;

/**
 * This UFS contract test will use Baidu Cloud BOS as the backing store.
 */
public final class BOSUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String BOS_BUCKET_CONF = "testBOSBucket";
  private static final String BOS_BUCKET = System.getProperty(BOS_BUCKET_CONF);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Preconditions.checkNotNull(BOS_BUCKET, "BOS_BUCKET");
    Preconditions.checkState(new BOSUnderFileSystemFactory().supportsPath(BOS_BUCKET),
        "%s is not a valid BOS path", BOS_BUCKET);
  }

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new BOSUnderFileSystemFactory().create(path, conf, mConfiguration);
  }

  @Override
  public String getUfsBaseDir() {
    return BOS_BUCKET;
  }
}
