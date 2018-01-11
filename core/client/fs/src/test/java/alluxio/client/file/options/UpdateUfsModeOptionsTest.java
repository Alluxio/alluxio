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

package alluxio.client.file.options;

import alluxio.test.util.CommonUtils;
import alluxio.thrift.UfsMode;
import alluxio.thrift.UpdateUfsModeTOptions;
import alluxio.underfs.UnderFileSystem;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link UpdateUfsModeOptions} class.
 */
public class UpdateUfsModeOptionsTest {
  @Test
  public void defaults() {
    UpdateUfsModeOptions options = UpdateUfsModeOptions.defaults();

    Assert.assertNotNull(options);
    Assert.assertEquals(UnderFileSystem.UfsMode.READ_WRITE, options.getUfsMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(UpdateUfsModeOptions.class);
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    UpdateUfsModeOptions options =
        UpdateUfsModeOptions.defaults().setUfsMode(UnderFileSystem.UfsMode.READ_ONLY);
    Assert.assertEquals(UnderFileSystem.UfsMode.READ_ONLY, options.getUfsMode());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    UpdateUfsModeOptions options =
        UpdateUfsModeOptions.defaults().setUfsMode(UnderFileSystem.UfsMode.NO_ACCESS);
    UpdateUfsModeTOptions thriftOptions = options.toThrift();

    Assert.assertEquals(UfsMode.NoAccess, thriftOptions.getUfsMode());
    Assert.assertEquals(UnderFileSystem.UfsMode.NO_ACCESS,
        new UpdateUfsModeOptions(thriftOptions).getUfsMode());
  }
}
