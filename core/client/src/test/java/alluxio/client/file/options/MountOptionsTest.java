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

import alluxio.CommonTestUtils;
import alluxio.thrift.MountTOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link MountOptions} class.
 */
public final class MountOptionsTest {
  /**
   * Tests creating a default {@link MountOptions} instance.
   */
  @Test
  public void defaults() {
    MountOptions options = MountOptions.defaults();
    Assert.assertFalse(options.isReadOnly());
    Assert.assertFalse(options.isShared());
  }

  @Test
  public void readOnlyField() {
    Assert.assertTrue(MountOptions.defaults().setReadOnly(true).isReadOnly());
    Assert.assertFalse(MountOptions.defaults().setReadOnly(false).isReadOnly());
  }

  @Test
  public void sharedField() {
    Assert.assertTrue(MountOptions.defaults().setShared(true).isShared());
    Assert.assertFalse(MountOptions.defaults().setShared(false).isShared());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    MountOptions options = MountOptions.defaults();
    MountTOptions thriftOptions = options.toThrift();
    Assert.assertFalse(thriftOptions.isReadOnly());

    options.setReadOnly(true);
    options.setShared(true);
    thriftOptions = options.toThrift();
    Assert.assertTrue(thriftOptions.isReadOnly());
    Assert.assertTrue(thriftOptions.isShared());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(MountOptions.class);
  }
}
