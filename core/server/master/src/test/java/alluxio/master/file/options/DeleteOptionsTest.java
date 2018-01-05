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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import alluxio.test.util.CommonUtils;

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
    assertFalse(options.isRecursive());
    assertFalse(options.isAlluxioOnly());
    assertFalse(options.isUnchecked());
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

    assertEquals(recursive, options.isRecursive());
    assertEquals(alluxioOnly, options.isAlluxioOnly());
    assertEquals(skipCheck, options.isUnchecked());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(DeleteOptions.class);
  }
}
