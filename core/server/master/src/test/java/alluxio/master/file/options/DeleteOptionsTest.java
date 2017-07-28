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

package alluxio.master.file.options;

import alluxio.CommonTestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link DeleteOptions}.
 */
public class DeleteOptionsTest {

  /**
   * Tests the {@link CreateFileOptions#defaults()} method.
   */
  @Test
  public void defaults() throws Exception {
    DeleteOptions options = DeleteOptions.defaults();
    Assert.assertFalse(options.isRecursive());
    Assert.assertFalse(options.isAlluxioOnly());
    Assert.assertFalse(options.isUnchecked());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    boolean recursive = random.nextBoolean();
    boolean alluxioOnly = random.nextBoolean();
    boolean skipCheck = random.nextBoolean();
    DeleteOptions options = DeleteOptions.defaults();

    options.setRecursive(recursive);
    options.setAlluxioOnly(alluxioOnly);
    options.setUnchecked(skipCheck);

    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(alluxioOnly, options.isAlluxioOnly());
    Assert.assertEquals(skipCheck, options.isUnchecked());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(DeleteOptions.class);
  }
}
