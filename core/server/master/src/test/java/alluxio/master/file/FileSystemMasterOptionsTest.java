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

package alluxio.master.file;

import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclPOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link alluxio.master.file.FileSystemMasterOptions}.
 */
public class FileSystemMasterOptionsTest {

  private FileSystemMasterOptions mMasterOptions;

  @Before
  public void init() {
    mMasterOptions = new DefaultFileSystemMasterOptions();
  }

  @Test
  public void listStatusOptionsDefaults() {
    ListStatusPOptions options = mMasterOptions.getListStatusOptions();
    Assert.assertNotNull(options);
    Assert.assertEquals(LoadMetadataPType.ONCE, options.getLoadMetadataType());
    Assert.assertEquals(false, options.getRecursive());
  }

  @Test
  public void loadMetadataOptionsDefaults() {
    LoadMetadataPOptions options = mMasterOptions.getLoadMetadataOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getCreateAncestors());
    Assert.assertEquals(options.getLoadDescendantType(), LoadDescendantPType.NONE);
  }

  @Test
  public void deleteOptionsDefaults() {
    DeletePOptions options = mMasterOptions.getDeleteOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
    Assert.assertFalse(options.getAlluxioOnly());
    Assert.assertFalse(options.getUnchecked());
  }

  @Test
  public void mountOptionsDefaults() {
    MountPOptions options = mMasterOptions.getMountOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getShared());
    Assert.assertFalse(options.getReadOnly());
    Assert.assertEquals(0, options.getPropertiesMap());
  }

  @Test
  public void setAclOptionsDefaults() {
    SetAclPOptions options = mMasterOptions.getSetAclOptions();
    Assert.assertNotNull(options);
    Assert.assertFalse(options.getRecursive());
  }
}
