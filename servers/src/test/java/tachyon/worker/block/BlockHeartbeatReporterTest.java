package tachyon.worker.block;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tachyon.worker.BlockStoreLocation;

import java.util.List;
import java.util.Map;

public class BlockHeartbeatReporterTest {
  private static final int USER_ID = 1;
  private static final BlockStoreLocation MEM_LOC = new BlockStoreLocation(1, 0);
  private static final BlockStoreLocation SSD_LOC = new BlockStoreLocation(2, 0);
  private static final BlockStoreLocation HDD_LOC = new BlockStoreLocation(3, 0);
  BlockHeartbeatReporter mReporter;

  @Before
  public final void before() {
    mReporter = new BlockHeartbeatReporter();
  }

  private void moveBlock(long blockId, BlockStoreLocation loc) {
    mReporter.postMoveBlock(USER_ID, blockId, loc);
  }

  private void removeBlock(long blockId) {
    mReporter.postRemoveBlock(USER_ID, blockId);
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
    Assert.assertEquals(addedBlocksMem.size(), 1);
    Assert.assertEquals(addedBlocksMem.get(0), block1);

    // Block2 moved to ssd
    List<Long> addedBlocksSsd = addedBlocks.get(SSD_LOC.getStorageDirId());
    Assert.assertEquals(addedBlocksSsd.size(), 1);
    Assert.assertEquals(addedBlocksSsd.get(0), block2);

    // Block3 moved to hdd
    List<Long> addedBlocksHdd = addedBlocks.get(HDD_LOC.getStorageDirId());
    Assert.assertEquals(addedBlocksHdd.size(), 1);
    Assert.assertEquals(addedBlocksHdd.get(0), block3);

    // All blocks should be "removed" to force an update of their location in master
    List<Long> removedBlocks = report.getRemovedBlocks();
    Assert.assertEquals(removedBlocks.size(), 3);
    Assert.assertTrue(removedBlocks.contains(block1));
    Assert.assertTrue(removedBlocks.contains(block2));
    Assert.assertTrue(removedBlocks.contains(block3));
  }

  // Tests generating a report clears the state of the reporter
  @Test
  public void generateReportStateClearTest() {
    Long block1 = 1L;
    moveBlock(block1, MEM_LOC);

    // First report should have updates
    BlockHeartbeatReport report = mReporter.generateReport();
    Assert.assertFalse(report.getAddedBlocks().isEmpty());
    Assert.assertFalse(report.getRemovedBlocks().isEmpty());

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
    Assert.assertEquals(removedBlocks.size(), 3);
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
    Assert.assertEquals(removedBlocks.size(), 1);
    Assert.assertTrue(removedBlocks.contains(block1));
  }
}
