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

package alluxio.underfs;

import alluxio.CommonTestUtils;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link UnderFileStatus} class.
 */
public final class UnderFileStatusTest {
  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean isDirectory = random.nextBoolean();
    UnderFileStatus status = new UnderFileStatus("name", isDirectory);

    Assert.assertEquals(isDirectory, status.isDirectory());
    Assert.assertEquals(!isDirectory, status.isFile()();
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(UnderFileStatus.class);
  }
}
