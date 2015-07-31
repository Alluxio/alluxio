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

package tachyon.worker.block;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlockHeartbeatReporterTest {
  private static final int USER_ID = 1;
  private static final BlockStoreLocation MEM_LOC = new BlockStoreLocation(1, 0, 0);
  private static final BlockStoreLocation SSD_LOC = new BlockStoreLocation(2, 1, 0);
  private static final BlockStoreLocation HDD_LOC = new BlockStoreLocation(3, 2, 0);
  BlockHeartbeatReporter mReporter;

  @Before
  public final void before() {
    mReporter = new BlockHeartbeatReporter();
  }

  private void moveBlock(long blockId, BlockStoreLocation newLocation) {
    BlockStoreLocation unusedOldLocation = new BlockStoreLocation(1, 0, 0);
    mReporter.onMoveBlockByWorker(USER_ID, blockId, unusedOldLocation, newLocation);
  }

  private void removeBlock(long blockId) {
    mReporter.onRemoveBlockByWorker(USER_ID, blockId);
  }

  // Tests Empty Report
  @Test
  public void generateReportEmptyTest() {
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertTrue(report.getAddedBlocks().isEmpty());
    Assert.assertTrue(report.getRemovedBlocks().isEmpty());
  }

  // Tests a report is correctly generated after moving blocks
  @Test
  public void generateReportMoveTest() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    moveBlock(block1, MEM_LOC);
    moveBlock(block2, SSD_LOC);
    moveBlock(block3, HDD_LOC);
    BlockHeartbeatReport report = mReporter.generateReport();
    Map<Long, List<Long>> addedBlocks = report.getAddedBlocks();

    // Block1 moved to memory
    List<Long> addedBlocksMem = addedBlocks.get(MEM_LOC.getStorageDirId());
    Assert.assertEquals(1, addedBlocksMem.size());
    Assert.assertEquals(block1, addedBlocksMem.get(0));

    // Block2 moved to ssd
    List<Long> addedBlocksSsd = addedBlocks.get(SSD_LOC.getStorageDirId());
    Assert.assertEquals(1, addedBlocksSsd.size());
    Assert.assertEquals(block2, addedBlocksSsd.get(0));

    // Block3 moved to hdd
    List<Long> addedBlocksHdd = addedBlocks.get(HDD_LOC.getStorageDirId());
    Assert.assertEquals(1, addedBlocksHdd.size());
    Assert.assertEquals(block3, addedBlocksHdd.get(0));
  }

  // Tests generating a report clears the state of the reporter
  @Test
  public void generateReportStateClearTest() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);

    // First report should have updates
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertFalse(report.getAddedBlocks().isEmpty());

    // Second report should not have updates
    BlockHeartbeatReport nextReport = mReporter.generateReport();
    Assert.assertTrue(nextReport.getAddedBlocks().isEmpty());
    Assert.assertTrue(nextReport.getRemovedBlocks().isEmpty());
  }

  // Tests a report is correctly generated after removing blocks
  @Test
  public void generateReportRemoveTest() {
    Long block1 = 1L;
    Long block2 = 2L;
    Long block3 = 3L;
    removeBlock(block1);
    removeBlock(block2);
    removeBlock(block3);
    BlockHeartbeatReport report = mReporter.generateReport();

    // All blocks should be removed
    List<Long> removedBlocks = report.getRemovedBlocks();
    Assert.assertEquals(3, removedBlocks.size());
    Assert.assertTrue(removedBlocks.contains(block1));
    Assert.assertTrue(removedBlocks.contains(block2));
    Assert.assertTrue(removedBlocks.contains(block3));

    // No blocks should have been added
    Map<Long, List<Long>> addedBlocks = report.getAddedBlocks();
    Assert.assertTrue(addedBlocks.isEmpty());
  }

  // Tests a report is correctly generated after moving and then removing a block
  @Test
  public void generateReportMoveThenRemoveTest() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);
    removeBlock(block1);

    // The block should not be in the added blocks list
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertTrue(report.getAddedBlocks().get(MEM_LOC.getStorageDirId()).isEmpty());

    // The block should be in the removed blocks list
    List<Long> removedBlocks = report.getRemovedBlocks();
    Assert.assertEquals(1, removedBlocks.size());
    Assert.assertTrue(removedBlocks.contains(block1));
  }
}
