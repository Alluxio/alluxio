package tachyon.worker.block.meta;

import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.evictor.EvictorTestUtils;

public class StorageDirViewTest {
  private static final int TEST_TIER_LEVEL = 0;
  private static final int TEST_DIR = 0;
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_BLOCK_SIZE = 20;
  private StorageDir mTestDir;
  private StorageDirView mTestDirView;
  private StorageTier mTestTier;
  private StorageTierView mTestTierView;
  private BlockMetadataManagerView mMetaManagerView;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    BlockMetadataManager metaManager =
        EvictorTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mMetaManagerView =
        Mockito.spy(new BlockMetadataManagerView(metaManager, Sets.<Integer>newHashSet(), Sets
            .<Long>newHashSet()));
    mTestTier = metaManager.getTiers().get(TEST_TIER_LEVEL);
    mTestDir = mTestTier.getDir(TEST_DIR);
    mTestTierView = new StorageTierView(mTestTier, mMetaManagerView);
    mTestDirView = new StorageDirView(mTestDir, mTestTierView, mMetaManagerView);
  }

  @Test
  public void getDirViewIndexTest() throws Exception {
    Assert.assertEquals(mTestDir.getDirIndex(), mTestDirView.getDirViewIndex());
  }

  @Test
  public void getParentTierViewTest() throws Exception {
    Assert.assertEquals(mTestTierView, mTestDirView.getParentTierView());
  }

  @Test
  public void toBlockStoreLocationTest() {
    Assert.assertEquals(mTestDir.toBlockStoreLocation(), mTestDirView.toBlockStoreLocation());
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Assert.assertEquals(mTestDir.getCapacityBytes(), mTestDirView.getCapacityBytes());
  }

  @Test
  public void getAvailableBytesTest() throws Exception {
    Assert.assertEquals(mTestDir.getAvailableBytes(), mTestDirView.getAvailableBytes());
  }

  @Test
  public void getCommittedBytesTest() throws Exception {
    Assert.assertEquals(mTestDir.getCommittedBytes(), mTestDirView.getCommittedBytes());
  }

  @Test
  public void getEvictableBlocksTest() throws Exception {
    // When test dir is empty, expect no block to be evictable
    Assert.assertEquals(0, mTestDirView.getEvitableBytes());
    Assert.assertEquals(0, mTestDirView.getEvictableBlocks().size());

    // Add one block to test dir, expect this block to be evictable
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mTestDir);
    mTestDir.addBlockMeta(blockMeta);
    Assert.assertEquals(TEST_BLOCK_SIZE, mTestDirView.getEvitableBytes());
    Assert.assertThat(mTestDirView.getEvictableBlocks(),
        is((List<BlockMeta>) Lists.newArrayList(blockMeta)));

    // Lock this block, expect this block to be non-evictable
    Mockito.when(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(false);
    Mockito.when(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(true);
    Assert.assertEquals(0, mTestDirView.getEvitableBytes());
    Assert.assertEquals(0, mTestDirView.getEvictableBlocks().size());

    // Pin this block, expect this block to be non-evictable
    Mockito.when(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(true);
    Mockito.when(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(false);
    Assert.assertEquals(0, mTestDirView.getEvitableBytes());
    Assert.assertEquals(0, mTestDirView.getEvictableBlocks().size());

    // Release pin/lock, expect this block to be evictable
    Mockito.when(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(false);
    Mockito.when(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(false);
    Assert.assertEquals(TEST_BLOCK_SIZE, mTestDirView.getEvitableBytes());
    Assert.assertThat(mTestDirView.getEvictableBlocks(),
        is((List<BlockMeta>) Lists.newArrayList(blockMeta)));
  }

  @Test
  public void createTempBlockMetaTest() throws Exception {
    TempBlockMeta tempBlockMeta =
        mTestDirView.createTempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE);
    Assert.assertEquals(TEST_USER_ID, tempBlockMeta.getUserId());
    Assert.assertEquals(TEST_TEMP_BLOCK_ID, tempBlockMeta.getBlockId());
    Assert.assertEquals(TEST_BLOCK_SIZE, tempBlockMeta.getBlockSize());
    Assert.assertEquals(mTestDir, tempBlockMeta.getParentDir());
  }
}
