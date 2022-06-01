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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link S3AUnderFileSystemFactory}.
 */
public class S3AUnderFileSystemFactoryTest {
  private String mS3APath = "s3a://test-bucket/path";
  private String mS3Path = "s3://test-bucket/path";
  private String mS3NPath = "s3n://test-bucket/path";
  private AlluxioConfiguration mAlluxioConf;
  private UnderFileSystemConfiguration mConf;
  private UnderFileSystemFactory mFactory1;

  @Before
  public void before() {
    mAlluxioConf = ConfigurationTestUtils.copyDefaults();
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
    mFactory1 = UnderFileSystemFactoryRegistry.find(mS3APath, mAlluxioConf);
  }

  @Test
  public void factory() {
    UnderFileSystemFactory factory2 = UnderFileSystemFactoryRegistry.find(
        mS3Path, mAlluxioConf);
    UnderFileSystemFactory factory3 = UnderFileSystemFactoryRegistry.find(
        mS3NPath, mAlluxioConf);

    assertNotNull(mFactory1);
    assertNotNull(factory2);
    assertNull(factory3);
  }

  @Test
  public void createInstanceWithNullPath() {
    Exception e = assertThrows(NullPointerException.class, () -> mFactory1.create(
        null, mConf));
    assertTrue(e.getMessage().contains("Unable to create UnderFileSystem instance: URI "
        + "path should not be null"));
  }

  @Test
  public void createInstanceWithPath() {
    UnderFileSystem ufs = mFactory1.create(mS3APath, mConf);
    assertNotNull(ufs);
    assertTrue(ufs instanceof S3AUnderFileSystem);
  }

  @Test
  public void supportsPath() {
    assertTrue(mFactory1.supportsPath(mS3APath));
    assertTrue(mFactory1.supportsPath(mS3Path));
    assertFalse(mFactory1.supportsPath(mS3NPath));
    assertFalse(mFactory1.supportsPath(null));
    assertFalse(mFactory1.supportsPath("Invalid_Path"));
    assertFalse(mFactory1.supportsPath("hdfs://test-bucket/path"));
  }
}
