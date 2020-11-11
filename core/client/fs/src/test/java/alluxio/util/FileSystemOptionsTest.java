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

package alluxio.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclPOptions;

import org.junit.Before;
import org.junit.Test;

public class FileSystemOptionsTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() throws Exception {
    mConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void loadMetadataOptionsDefaults() {
    LoadMetadataPOptions options = FileSystemOptions.loadMetadataDefaults(mConf);
    assertNotNull(options);
    assertFalse(options.getCreateAncestors());
    assertFalse(options.getRecursive());
    assertEquals(options.getLoadDescendantType(), LoadDescendantPType.NONE);
  }

  @Test
  public void deleteOptionsDefaults() {
    DeletePOptions options = FileSystemOptions.deleteDefaults(mConf);
    assertNotNull(options);
    assertFalse(options.getRecursive());
    assertFalse(options.getAlluxioOnly());
    assertFalse(options.getUnchecked());
  }

  @Test
  public void mountOptionsDefaults() {
    MountPOptions options = FileSystemOptions.mountDefaults(mConf);
    assertNotNull(options);
    assertFalse(options.getShared());
    assertFalse(options.getReadOnly());
    assertEquals(0, options.getPropertiesMap().size());
  }

  @Test
  public void setAclOptionsDefaults() {
    SetAclPOptions options = FileSystemOptions.setAclDefaults(mConf);
    assertNotNull(options);
    assertFalse(options.getRecursive());
  }
}
