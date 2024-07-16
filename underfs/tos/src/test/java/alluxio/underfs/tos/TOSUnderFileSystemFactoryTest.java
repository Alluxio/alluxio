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

package alluxio.underfs.tos;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TOSUnderFileSystemFactoryTest {
  private final String mTOSPath = "tos://tos-alluxio-test/";
  private InstancedConfiguration mAlluxioConf;
  private UnderFileSystemConfiguration mConf;
  private UnderFileSystemFactory mFactory1;

  @Before
  public void setUp() {
    mAlluxioConf = Mockito.mock(InstancedConfiguration.class);
    Mockito.when(mAlluxioConf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))
        .thenReturn("tos://tos-alluxio-test/");
    Mockito.when(mAlluxioConf.get(PropertyKey.TOS_ACCESS_KEY))
        .thenReturn("mockAccessKey");
    Mockito.when(mAlluxioConf.get(PropertyKey.TOS_SECRET_KEY))
        .thenReturn("mockSecretKey");
    Mockito.when(mAlluxioConf.get(PropertyKey.TOS_ENDPOINT_KEY))
        .thenReturn("tos-cn-beijing.volces.com");
    Mockito.when(mAlluxioConf.get(PropertyKey.TOS_REGION))
        .thenReturn("cn-beijing");

    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);
    mFactory1 = UnderFileSystemFactoryRegistry.find(mTOSPath, mAlluxioConf);
  }

  @Test
  public void factory() {
    assertNotNull(mFactory1);
  }

  @Test
  public void createInstanceWithNullPath() {
    Exception e = assertThrows(NullPointerException.class, () -> mFactory1.create(null, mConf));
    assertTrue(e.getMessage().contains("path"));
  }

  @Test
  public void supportsPath() {
    assertTrue(mFactory1.supportsPath(mTOSPath));
    assertFalse(mFactory1.supportsPath("s3a://test-bucket/path"));
    assertFalse(mFactory1.supportsPath("InvalidPath"));
    assertFalse(mFactory1.supportsPath(null));
  }

  @Test
  public void createInstanceWithMissingCredentials() {
    Mockito.when(mAlluxioConf.get(PropertyKey.TOS_ACCESS_KEY)).thenReturn(null);
    Mockito.when(mAlluxioConf.get(PropertyKey.TOS_SECRET_KEY)).thenReturn(null);
    mConf = UnderFileSystemConfiguration.defaults(mAlluxioConf);

    Exception e = assertThrows(RuntimeException.class, () -> mFactory1.create(mTOSPath, mConf));
    assertTrue(e.getMessage().contains("TOS Credentials not available"));
  }
}
