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

package alluxio.master.file.options;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MountOptionsTest}.
 */
public class MountOptionsTest {
  /**
   * Tests the {@link MountOptions#defaults()} method.
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
    MountOptions options = MountOptions.defaults().setReadOnly(true);
    Assert.assertTrue(options.isReadOnly());

    options = MountOptions.defaults().setReadOnly(false);
    Assert.assertFalse(options.isReadOnly());
  }
}
