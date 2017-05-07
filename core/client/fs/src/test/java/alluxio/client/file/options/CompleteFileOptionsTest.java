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
import alluxio.thrift.CompleteFileTOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link CompleteFileOptions} class.
 */
public final class CompleteFileOptionsTest {
  /**
   * Tests that building a {@link CompleteFileOptions} with the defaults works.
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
    long len = new Random().nextLong();
    CompleteFileOptions options = CompleteFileOptions.defaults();
    options.setUfsLength(len);
    Assert.assertEquals(len, options.getUfsLength());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    long len = new Random().nextLong();
    CompleteFileOptions options = CompleteFileOptions.defaults();
    options.setUfsLength(len);
    CompleteFileTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(len, thriftOptions.getUfsLength());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CompleteFileOptions.class);
  }
}
