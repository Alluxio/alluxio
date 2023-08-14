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

package alluxio.underfs.obs;

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
 * Unit tests for the {@link OBSUnderFileSystemFactory}.
 */
public class OBSUnderFileSystemFactoryTest {

  /**
   * Tests that the OBS UFS module correctly accepts paths that begin with obs://.
   */

  private final String mObsPath = "obs://test-bucket/path";
  private final String mObsAccessKey = "test-access-key";
  private final String mObsSecretKey = "test-secret-key";
  private final String mObsEndpoint = "test-endpoint";
  private AlluxioConfiguration mAlluxioConf;
  private UnderFileSystemConfiguration mConf;
  private UnderFileSystemFactory mFactory;

  @Before
  public void setUp() {
    Configuration.set(PropertyKey.OBS_ACCESS_KEY, mObsAccessKey);
    Configuration.set(PropertyKey.OBS_SECRET_KEY, mObsSecretKey);
    Configuration.set(PropertyKey.OBS_ENDPOINT, mObsEndpoint);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
    mFactory = UnderFileSystemFactoryRegistry.find(mObsPath, mConf);
  }

  @Test
  public void factory() {
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for obs paths when using this module", mFactory);
  }

  /**
   * Test case for {@link OBSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithNullPath() {
    Exception e = Assert.assertThrows(NullPointerException.class, () -> mFactory.create(
        null, mConf));
    Assert.assertTrue(e.getMessage().contains("Unable to create UnderFileSystem instance: URI "
        + "path should not be null"));
  }

  /**
   * Test case for {@link OBSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithPath() {
    UnderFileSystem ufs = mFactory.create(mObsPath, mConf);
    Assert.assertNotNull(ufs);
    Assert.assertTrue(ufs instanceof OBSUnderFileSystem);
  }

  /**
   * Test case for {@link OBSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithoutCredentials() {
    Configuration.unset(PropertyKey.OBS_ACCESS_KEY);
    Configuration.unset(PropertyKey.OBS_SECRET_KEY);
    Configuration.unset(PropertyKey.OBS_ENDPOINT);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);

    try {
      mFactory.create(mObsPath, mConf);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertTrue(e.getMessage().contains("OBS credentials or endpoint not available, "
          + "cannot create OBS Under File System."));
    }
    Exception e = Assert.assertThrows(RuntimeException.class, () -> mFactory.create(
        mObsPath, mConf));
    Assert.assertTrue(e.getMessage().contains("OBS credentials or endpoint not available, "
        + "cannot create OBS Under File System."));
  }

  /**
   * Test case for {@link OBSUnderFileSystemFactory#supportsPath(String)}.
   */
  @Test
  public void supportsPath() {
    Assert.assertTrue(mFactory.supportsPath(mObsPath));
    assertFalse(mFactory.supportsPath(null));
    assertFalse(mFactory.supportsPath("Invalid_Path"));
    assertFalse(mFactory.supportsPath("hdfs://test-bucket/path"));
  }
}
