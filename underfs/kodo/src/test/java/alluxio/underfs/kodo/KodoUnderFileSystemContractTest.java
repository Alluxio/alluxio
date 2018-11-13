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

package alluxio.underfs.kodo;

import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Preconditions;

import org.junit.BeforeClass;

public class KodoUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {

  private static final String KODO_BUCKET_CONF = "testKodoBucket";
  private static final String KODO_BUCKET = System.getProperty(KODO_BUCKET_CONF);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Preconditions.checkNotNull(KODO_BUCKET, "KODO_BUCKET");
    Preconditions.checkState(new KodoUnderFileSystemFactory().supportsPath(KODO_BUCKET),
        "%s is not a valid KODO path", KODO_BUCKET);
  }

  /**
   * @param path path of UFS
   * @param conf UFS configuration
   * @return the UFS instance for test
   */
  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new KodoUnderFileSystemFactory().create(path, conf);
  }

  /**
   * @return the UFS address for test
   */
  @Override
  public String getUfsBaseDir() {
    return KODO_BUCKET;
  }
}
