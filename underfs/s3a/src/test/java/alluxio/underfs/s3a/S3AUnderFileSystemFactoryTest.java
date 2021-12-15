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

package alluxio.underfs.s3a;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link S3AUnderFileSystemFactory}.
 */
public class S3AUnderFileSystemFactoryTest {
  private String mPath = "s3a://test-bucket/path";
  private AlluxioConfiguration mAlluxioConf = ConfigurationTestUtils.defaults();
  private UnderFileSystemConfiguration mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
  private UnderFileSystemFactory mFactory1 = UnderFileSystemFactoryRegistry.find(
          "s3a://test-bucket/path", mAlluxioConf);
  private UnderFileSystemFactory mFactory2 = UnderFileSystemFactoryRegistry.find(
          "s3://test-bucket/path", mAlluxioConf);
  private UnderFileSystemFactory mFactory3 = UnderFileSystemFactoryRegistry.find(
          "s3n://test-bucket/path", mAlluxioConf);

  @Test
  public void factory() {
    assertNotNull(mFactory1);
    assertNotNull(mFactory2);
    assertNull(mFactory3);
  }

  @Test
  public void createInstanceWithNullPath() {
    Exception e = Assert.assertThrows(NullPointerException.class, () -> mFactory1.create(
            null, mConf));
    assertTrue(e.getMessage().equals("path should not be null"));
  }

  @Test
  public void createInstanceWithPath() {
    UnderFileSystem ufs = mFactory1.create(mPath, mConf);
    assertNotNull(ufs);
    assertTrue(ufs instanceof S3AUnderFileSystem);
  }

  @Test
  public void supportsValidPath() {
    assertTrue(mFactory1.supportsPath(mPath));
  }
}
