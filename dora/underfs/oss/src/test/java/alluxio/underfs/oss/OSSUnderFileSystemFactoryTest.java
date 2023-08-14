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
 * Unit tests for the {@link OSSUnderFileSystemFactory}.
 */
public class OSSUnderFileSystemFactoryTest {

  /**
   * Tests that the OSS UFS module correctly accepts paths that begin with oss://.
   */

  private final String mOssPath = "oss://test-bucket/path";
  private final String mOssAccessKey = "test-access-key";
  private final String mOssSecretKey = "test-secret-key";
  private final String mOssEndpoint = "test-endpoint";
  private AlluxioConfiguration mAlluxioConf;
  private UnderFileSystemConfiguration mConf;
  private UnderFileSystemFactory mFactory;

  @Before
  public void setUp() {
    Configuration.set(PropertyKey.OSS_ACCESS_KEY, mOssAccessKey);
    Configuration.set(PropertyKey.OSS_SECRET_KEY, mOssSecretKey);
    Configuration.set(PropertyKey.OSS_ENDPOINT_KEY, mOssEndpoint);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
    mFactory = UnderFileSystemFactoryRegistry.find(mOssPath, mConf);
  }

  @Test
  public void factory() {
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for oss paths when using this module", mFactory);
  }

  /**
   * Test case for {@link OSSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithNullPath() {
    Exception e = Assert.assertThrows(NullPointerException.class, () -> mFactory.create(
        null, mConf));
    Assert.assertTrue(e.getMessage().contains("Unable to create UnderFileSystem instance: URI "
        + "path should not be null"));
  }

  /**
   * Test case for {@link OSSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithPath() {
    UnderFileSystem ufs = mFactory.create(mOssPath, mConf);
    Assert.assertNotNull(ufs);
    Assert.assertTrue(ufs instanceof OSSUnderFileSystem);
  }

  /**
   * Test case for {@link OSSUnderFileSystemFactory#create(String, UnderFileSystemConfiguration)}.
   */
  @Test
  public void createInstanceWithoutCredentials() {
    Configuration.unset(PropertyKey.OSS_ACCESS_KEY);
    Configuration.unset(PropertyKey.OSS_SECRET_KEY);
    Configuration.unset(PropertyKey.OSS_ENDPOINT_KEY);
    mAlluxioConf = Configuration.global();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);

    try {
      mFactory.create(mOssPath, mConf);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertTrue(e.getMessage().contains("OSS Credentials not available, "
          + "cannot create OSS Under File System."));
    }
    Exception e = Assert.assertThrows(RuntimeException.class, () -> mFactory.create(
        mOssPath, mConf));
    Assert.assertTrue(e.getMessage().contains("OSS Credentials not available, "
        + "cannot create OSS Under File System."));
  }

  /**
   * Test case for {@link OSSUnderFileSystemFactory#supportsPath(String)}.
   */
  @Test
  public void supportsPath() {
    Assert.assertTrue(mFactory.supportsPath(mOssPath));
    assertFalse(mFactory.supportsPath(null));
    assertFalse(mFactory.supportsPath("Invalid_Path"));
    assertFalse(mFactory.supportsPath("hdfs://test-bucket/path"));
  }
}
