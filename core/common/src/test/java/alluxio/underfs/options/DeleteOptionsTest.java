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

package alluxio.underfs.options;

import static org.junit.Assert.assertEquals;

import alluxio.test.util.CommonUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Tests for the {@link DeleteOptions} class.
 */
@RunWith(PowerMockRunner.class)
public final class DeleteOptionsTest {
  /**
   * Tests for default {@link DeleteOptions}.
   */
  @Test
  public void defaults() throws IOException {
    DeleteOptions options = DeleteOptions.defaults();

    assertEquals(false, options.isRecursive());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    DeleteOptions options = DeleteOptions.defaults();

    boolean recursive = false;
    options.setRecursive(recursive);
    assertEquals(recursive, options.isRecursive());

    recursive = true;
    options.setRecursive(recursive);
    assertEquals(recursive, options.isRecursive());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(DeleteOptions.class);
  }
}
