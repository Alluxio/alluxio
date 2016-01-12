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

public class BlockContainerIdGeneratorTest {
  private BlockContainerIdGenerator mGenerator;

  @Before
  public void before() throws Exception {
    mGenerator = new BlockContainerIdGenerator();
  }

  @Test
  public void getNewContainerIdTest() {
    Assert.assertEquals(0, mGenerator.getNewContainerId());
    Assert.assertEquals(1, mGenerator.getNewContainerId());
    Assert.assertEquals(2, mGenerator.getNewContainerId());
  }

  @Test
  public void setNextContainerIdTest() {
    mGenerator.setNextContainerId(123);
    Assert.assertEquals(123, mGenerator.getNewContainerId());
    Assert.assertEquals(124, mGenerator.getNewContainerId());
    Assert.assertEquals(125, mGenerator.getNewContainerId());
  }

  @Test
  public void toJournalEntryTest() {
    mGenerator.setNextContainerId(123);
    Journal.JournalEntry entry = mGenerator.toJournalEntry();
    Assert.assertTrue(entry.hasBlockContainerIdGenerator());
    Block.BlockContainerIdGeneratorEntry generatorEntry = entry.getBlockContainerIdGenerator();
    Assert.assertNotNull(generatorEntry);
    Assert.assertEquals(123, generatorEntry.getNextContainerId());
  }
}
