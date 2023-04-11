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

package alluxio.master.block;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.MasterUtils;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metastore.MetastoreType;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.journal.Block;
import alluxio.proto.journal.Journal;
import alluxio.wire.BlockInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class DefaultBlockMasterCheckpointTest {
  @Parameterized.Parameters
  public static Collection<MetastoreType> data() {
    return Arrays.asList(MetastoreType.HEAP, MetastoreType.ROCKS);
  }

  @Parameterized.Parameter
  public MetastoreType mMetastoreType;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private DefaultBlockMaster mBlockMaster;

  private final long mNextContainerId = 1;
  private final long mBlockId1 = 2;
  private final long mBlockId2 = 3;
  private final long mBlockLength = 4;

  private DefaultBlockMaster createDefaultBlockMaster() throws IOException {
    CoreMasterContext context = MasterTestUtils.testMasterContext(new NoopJournalSystem(), null,
        MasterUtils.getBlockStoreFactory(mFolder.newFolder().getAbsolutePath()),
        MasterUtils.getInodeStoreFactory(mFolder.newFolder().getAbsolutePath()));
    MetricsMasterFactory metricsMasterFactory = new MetricsMasterFactory();
    MetricsMaster metricsMaster = metricsMasterFactory.create(new MasterRegistry(), context);
    return new DefaultBlockMaster(metricsMaster, context);
  }

  @Before
  public void before() throws IOException {
    Configuration.set(PropertyKey.MASTER_BLOCK_METASTORE, mMetastoreType);
    mBlockMaster = createDefaultBlockMaster();
    mBlockMaster.processJournalEntry(Journal.JournalEntry.newBuilder()
        .setBlockContainerIdGenerator(Block.BlockContainerIdGeneratorEntry.newBuilder()
            .setNextContainerId(mNextContainerId)).build());
    mBlockMaster.processJournalEntry(Journal.JournalEntry.newBuilder()
        .setBlockInfo(Block.BlockInfoEntry.newBuilder()
            .setBlockId(mBlockId1)).build());
    mBlockMaster.processJournalEntry(Journal.JournalEntry.newBuilder()
        .setBlockInfo(Block.BlockInfoEntry.newBuilder()
            .setBlockId(mBlockId2)
            .setLength(mBlockLength)).build());
    mBlockMaster.processJournalEntry(Journal.JournalEntry.newBuilder()
        .setDeleteBlock(Block.DeleteBlockEntry.newBuilder()
            .setBlockId(mBlockId1)).build());
  }

  @Test
  public void testOutputStream() throws IOException, InterruptedException, BlockInfoException {
    File file = mFolder.newFile();
    try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
      mBlockMaster.writeToCheckpoint(outputStream);
    }
    DefaultBlockMaster blockMaster = createDefaultBlockMaster();
    try (CheckpointInputStream inputStream =
             new CheckpointInputStream(Files.newInputStream(file.toPath()))) {
      blockMaster.restoreFromCheckpoint(inputStream);
    }
    Assert.assertEquals(mNextContainerId, blockMaster.getJournaledNextContainerId());
    Assert.assertThrows(BlockInfoException.class, () -> blockMaster.getBlockInfo(mBlockId1));
    BlockInfo blockInfo = blockMaster.getBlockInfo(mBlockId2);
    Assert.assertEquals(mBlockLength, blockInfo.getLength());
  }

  @Test
  public void testDirectory() throws IOException, BlockInfoException {
    File dir = mFolder.newFolder();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    mBlockMaster.writeToCheckpoint(dir, executor).join();
    DefaultBlockMaster blockMaster = createDefaultBlockMaster();
    blockMaster.restoreFromCheckpoint(dir, executor).join();

    Assert.assertEquals(mNextContainerId, blockMaster.getJournaledNextContainerId());
    Assert.assertThrows(BlockInfoException.class, () -> blockMaster.getBlockInfo(mBlockId1));
    BlockInfo blockInfo = blockMaster.getBlockInfo(mBlockId2);
    Assert.assertEquals(mBlockLength, blockInfo.getLength());
  }
}
