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

package alluxio.master.block.meta;

import alluxio.master.block.BlockContainerIdGenerator;
import alluxio.proto.journal.Block;
import alluxio.proto.journal.Journal;

import org.junit.Assert;
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
  public void getNewContainerIdTest() {
    // The default container id is 0.
    Assert.assertEquals(0, mGenerator.getNewContainerId());
    Assert.assertEquals(1, mGenerator.getNewContainerId());
    Assert.assertEquals(2, mGenerator.getNewContainerId());
  }

  /**
   * Tests the {@link BlockContainerIdGenerator#setNextContainerId(long)} method.
   */
  @Test
  public void setNextContainerIdTest() {
    mGenerator.setNextContainerId(TEST_ID);
    Assert.assertEquals(TEST_ID, mGenerator.getNewContainerId());
    Assert.assertEquals(TEST_ID + 1, mGenerator.getNewContainerId());
    Assert.assertEquals(TEST_ID + 2, mGenerator.getNewContainerId());
  }

  /**
   * Tests the {@link BlockContainerIdGenerator#toJournalEntry()} method.
   */
  @Test
  public void toJournalEntryTest() {
    mGenerator.setNextContainerId(TEST_ID);
    Journal.JournalEntry entry = mGenerator.toJournalEntry();
    Assert.assertNotNull(entry);
    Assert.assertTrue(entry.hasBlockContainerIdGenerator());
    Block.BlockContainerIdGeneratorEntry generatorEntry = entry.getBlockContainerIdGenerator();
    Assert.assertNotNull(generatorEntry);
    Assert.assertEquals(TEST_ID, generatorEntry.getNextContainerId());
  }
}
