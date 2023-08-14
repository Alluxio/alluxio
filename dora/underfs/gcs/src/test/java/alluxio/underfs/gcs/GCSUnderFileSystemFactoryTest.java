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

package alluxio.underfs.gcs;

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
 * Unit tests for the {@link GCSUnderFileSystemFactory}.
 */
public final class GCSUnderFileSystemFactoryTest {

  /**
   * This test ensures the GCS UFS module correctly accepts paths that begin with gs://.
   */

  private final String mPath = "gs://test-bucket/path";
  private final String mAccessKey = "test-access-key";
  private final String mSecretKey = "test-secret-key";
  private AlluxioConfiguration mAlluxioConf;
  private UnderFileSystemConfiguration mConf;
  private UnderFileSystemFactory mFactory;

  @Before
  public void setUp() {
    Configuration.set(PropertyKey.GCS_ACCESS_KEY, mAccessKey);
    Configuration.set(PropertyKey.GCS_SECRET_KEY, mSecretKey);
    Configuration.set(PropertyKey.UNDERFS_GCS_VERSION, 1);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
    mFactory = UnderFileSystemFactoryRegistry.find(mPath, mConf);
  }

  @Test
  public void factory() {
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for gcs paths when using this module", mFactory);
  }

  /**
   * Test case for {@link GCSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithNullPath() {
    Exception e = Assert.assertThrows(NullPointerException.class, () -> mFactory.create(
        null, mConf));
    Assert.assertTrue(e.getMessage().contains("Unable to create UnderFileSystem instance: URI "
        + "path should not be null"));
  }

  /**
   * Test case for {@link GCSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithPath() {
    UnderFileSystem ufs = mFactory.create(mPath, mConf);
    Assert.assertNotNull(ufs);
    Assert.assertTrue(ufs instanceof GCSUnderFileSystem);
  }

  /**
   * Test case for {@link GCSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithoutCredentials() {
    Configuration.unset(PropertyKey.GCS_ACCESS_KEY);
    Configuration.unset(PropertyKey.GCS_SECRET_KEY);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);

    try {
      mFactory.create(mPath, mConf);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertTrue(e.getMessage().contains("GCS credentials or version not available, "
          + "cannot create GCS Under File System."));
    }
    Exception e = Assert.assertThrows(RuntimeException.class, () -> mFactory.create(
        mPath, mConf));
    Assert.assertTrue(e.getMessage().contains("GCS credentials or version not available, "
        + "cannot create GCS Under File System."));
  }

  /**
   * Test case for {@link GCSUnderFileSystemFactory#supportsPath(String)}.
   */
  @Test
  public void supportsPath() {
    Assert.assertTrue(mFactory.supportsPath(mPath));
    assertFalse(mFactory.supportsPath(null));
    assertFalse(mFactory.supportsPath("Invalid_Path"));
    assertFalse(mFactory.supportsPath("hdfs://test-bucket/path"));
  }
}
