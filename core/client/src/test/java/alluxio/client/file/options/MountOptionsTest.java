/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.options;

import alluxio.thrift.MountTOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link MountOptions} class.
 */
public class MountOptionsTest {
  /**
   * Tests creating a default {@link MountOptions} instance.
   */
  @Test
  public void defaultsTest() {
    MountOptions options = MountOptions.defaults();
    Assert.assertFalse(options.isReadOnly());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    MountOptions options = MountOptions.defaults();
    options.setReadOnly(true);
    Assert.assertTrue(options.isReadOnly());
    options.setReadOnly(false);
    Assert.assertFalse(options.isReadOnly());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThriftTest() {
    MountOptions options = MountOptions.defaults();
    MountTOptions thriftOptions = options.toThrift();
    Assert.assertFalse(thriftOptions.isReadOnly());

    options.setReadOnly(true);
    thriftOptions = options.toThrift();
    Assert.assertTrue(thriftOptions.isReadOnly());
  }
}
