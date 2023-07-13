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

package alluxio.underfs.cos;

import static org.junit.Assert.assertFalse;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link COSUnderFileSystemFactory}.
 */
public class COSUnderFileSystemFactoryTest {

  /**
   * Tests that the COS UFS module correctly accepts paths that begin with cos://.
   */

  private final String mCosPath = "cos://test-bucket/path";
  private final String mCosAccessKey = "test-access-key";
  private final String mCosSecretKey = "test-secret-key";
  private final String mCosRegion = "test-region";
  private final String mCosAppId = "test-app-id";
  private AlluxioConfiguration mAlluxioConf;
  private UnderFileSystemConfiguration mConf;
  private UnderFileSystemFactory mFactory;

  @Before
  public void setUp() {
    Configuration.set(PropertyKey.COS_ACCESS_KEY, mCosAccessKey);
    Configuration.set(PropertyKey.COS_SECRET_KEY, mCosSecretKey);
    Configuration.set(PropertyKey.COS_REGION, mCosRegion);
    Configuration.set(PropertyKey.COS_APP_ID, mCosAppId);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
    mFactory = UnderFileSystemFactoryRegistry.find(mCosPath, mConf);
  }

  @Test
  public void factory() {
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for cos paths when using this module", mFactory);
  }

  /**
   * Test case for {@link COSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithNullPath() {
    Exception e = Assert.assertThrows(NullPointerException.class, () -> mFactory.create(
        null, mConf));
    Assert.assertTrue(e.getMessage().contains("Unable to create UnderFileSystem instance: URI "
        + "path should not be null"));
  }

  /**
   * Test case for {@link COSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithPath() {
    UnderFileSystem ufs = mFactory.create(mCosPath, mConf);
    Assert.assertNotNull(ufs);
    Assert.assertTrue(ufs instanceof COSUnderFileSystem);
  }

  /**
   * Test case for {@link COSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithoutCredentials() {
    Configuration.unset(PropertyKey.COS_ACCESS_KEY);
    Configuration.unset(PropertyKey.COS_SECRET_KEY);
    Configuration.unset(PropertyKey.COS_REGION);
    Configuration.unset(PropertyKey.COS_APP_ID);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);

    try {
      mFactory.create(mCosPath, mConf);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertTrue(e.getMessage().contains("COS Credentials not available, "
          + "cannot create COS Under File System."));
    }
    Exception e = Assert.assertThrows(RuntimeException.class, () -> mFactory.create(
        mCosPath, mConf));
    Assert.assertTrue(e.getMessage().contains("COS Credentials not available, "
        + "cannot create COS Under File System."));
  }

  /**
   * Test case for {@link COSUnderFileSystemFactory#supportsPath(String)}.
   */
  @Test
  public void supportsPath() {
    Assert.assertTrue(mFactory.supportsPath(mCosPath));
    assertFalse(mFactory.supportsPath(null));
    assertFalse(mFactory.supportsPath("Invalid_Path"));
    assertFalse(mFactory.supportsPath("hdfs://test-bucket/path"));
  }
}
