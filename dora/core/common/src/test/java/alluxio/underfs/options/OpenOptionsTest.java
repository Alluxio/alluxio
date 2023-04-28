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
 * Tests for the {@link OpenOptions} class.
 */
@RunWith(PowerMockRunner.class)
public final class OpenOptionsTest {
  /**
   * Tests for default {@link OpenOptions}.
   */
  @Test
  public void defaults() throws IOException {
    OpenOptions options = OpenOptions.defaults();

    assertEquals(0, options.getOffset());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    OpenOptions options = OpenOptions.defaults();

    long[] offsets = {100, 110, 150, 200};
    for (long offset : offsets) {
      options.setOffset(offset);
      assertEquals(offset, options.getOffset());
    }
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(OpenOptions.class);
  }
}
