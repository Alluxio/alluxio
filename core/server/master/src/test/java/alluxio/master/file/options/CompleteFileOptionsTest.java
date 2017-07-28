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
 * Unit tests for {@link CompleteFileOptions}.
 */
public final class CompleteFileOptionsTest {
  /**
   * Tests the {@link CompleteFileOptions#defaults()} method.
   */
  @Test
  public void defaults() {
    CompleteFileOptions options = CompleteFileOptions.defaults();

    Assert.assertEquals(0, options.getUfsLength());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long ufsLength = random.nextLong();
    long operationTimeMs = random.nextLong();

    CompleteFileOptions options =
        CompleteFileOptions.defaults().setUfsLength(ufsLength).setOperationTimeMs(operationTimeMs);

    Assert.assertEquals(ufsLength, options.getUfsLength());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CompleteFileOptions.class);
  }
}
