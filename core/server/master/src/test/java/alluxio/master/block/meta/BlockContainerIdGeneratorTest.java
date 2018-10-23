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

package alluxio.master.block.meta;

import static org.junit.Assert.assertEquals;

import alluxio.master.block.BlockContainerIdGenerator;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link BlockContainerIdGenerator}.
 */
public final class BlockContainerIdGeneratorTest {
  /** A non-default, container id for testing. */
  private static final long TEST_ID = 1234;
  private BlockContainerIdGenerator mGenerator;

  /**
   * Sets up a new {@link BlockContainerIdGenerator} before a test runs.
   */
  @Before
  public void before() {
    mGenerator = new BlockContainerIdGenerator();
  }

  /**
   * Tests the {@link BlockContainerIdGenerator#getNewContainerId()} method.
   */
  @Test
  public void getNewContainerId() {
    // The default container id is 0.
    assertEquals(0, mGenerator.getNewContainerId());
    assertEquals(1, mGenerator.getNewContainerId());
    assertEquals(2, mGenerator.getNewContainerId());
  }

  /**
   * Tests the {@link BlockContainerIdGenerator#setNextContainerId(long)} method.
   */
  @Test
  public void setNextContainerId() {
    mGenerator.setNextContainerId(TEST_ID);
    assertEquals(TEST_ID, mGenerator.getNewContainerId());
    assertEquals(TEST_ID + 1, mGenerator.getNewContainerId());
    assertEquals(TEST_ID + 2, mGenerator.getNewContainerId());
  }
}
