/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.block.meta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.master.block.BlockContainerIdGenerator;
import tachyon.proto.journal.Block;
import tachyon.proto.journal.Journal;

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
