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

package tachyon.master.lineage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import tachyon.TachyonURI;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidPathException;
import tachyon.exception.LineageDeletionException;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.master.lineage.checkpoint.CheckpointPlan;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.lineage.meta.LineageFileState;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageInfo;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class LineageMasterTest {
  private LineageMaster mLineageMaster;
  private FileSystemMaster mFileSystemMaster;
  private Job mJob;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    Journal journal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mLineageMaster = new LineageMaster(mFileSystemMaster, journal);
    mLineageMaster.start(true);
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  @Test
  public void listLineagesTest() throws Exception {
    mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new TachyonURI("/test1")),
        Lists.newArrayList(new TachyonURI("/test2")), mJob);
    List<LineageInfo> info = mLineageMaster.getLineageInfoList();
    Assert.assertEquals(2, info.size());
  }

  @Test
  public void createLineageWithNonExistingFileTest() throws Exception {
    TachyonURI missingInput = new TachyonURI("/test1");
    Mockito.when(mFileSystemMaster.getFileId(missingInput)).thenReturn(-1L);
    // try catch block used because ExpectedExceptionRule conflicts with Powermock
    try {
      mLineageMaster.createLineage(Lists.newArrayList(missingInput),
          Lists.newArrayList(new TachyonURI("/test2")), mJob);
      Assert.fail();
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.LINEAGE_INPUT_FILE_NOT_EXIST.getMessage("/test1"),
          e.getMessage());
    }
  }

  @Test
  public void deleteLineageTest() throws Exception {
    long l1 = mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new TachyonURI("/test1")),
        Lists.newArrayList(new TachyonURI("/test2")), mJob);
    mLineageMaster.deleteLineage(l1, true);
    List<LineageInfo> info = mLineageMaster.getLineageInfoList();
    Assert.assertEquals(0, info.size());
  }

  @Test
  public void deleteNonexistingLineageTest() throws Exception {
    long id = 1L;
    try {
      mLineageMaster.deleteLineage(id, false);
      Assert.fail();
    } catch (LineageDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.LINEAGE_DOES_NOT_EXIST.getMessage(id), e.getMessage());
    }
  }

  @Test
  public void deleteLineageWithChildrenTest() throws Exception {
    long l1 = mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new TachyonURI("/test1")),
        Lists.newArrayList(new TachyonURI("/test2")), mJob);
    try {
      mLineageMaster.deleteLineage(l1, false);
      Assert.fail();
    } catch (LineageDeletionException e) {
      Assert.assertEquals(ExceptionMessage.DELETE_LINEAGE_WITH_CHILDREN.getMessage(l1),
          e.getMessage());
    }
  }

  @Test
  public void reinitializeFileTest() throws Exception {
    mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    mLineageMaster.reinitializeFile("/test1", 500L, 10L);
    Mockito.verify(mFileSystemMaster).reinitializeFile(new TachyonURI("/test1"), 500L, 10L);
  }

  @Test
  public void asyncCompleteFileTest() throws Exception {
    long fileId = 0;
    mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    mFileSystemMaster.completeFile(fileId);
    Mockito.verify(mFileSystemMaster).completeFile(fileId);
  }

  @Test
  public void heartbeatTest() throws Exception {
    long fileId = 0;
    long workerId = 1;
    long blockId = 2;

    mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    Set<LineageFile> checkpointFiles = Sets.newHashSet();
    checkpointFiles.add(new LineageFile(fileId, LineageFileState.COMPLETED));
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.blockInfo = new BlockInfo();
    fileBlockInfo.blockInfo.blockId = blockId;
    Mockito.when(mFileSystemMaster.getFileBlockInfoList(fileId))
        .thenReturn(Lists.newArrayList(fileBlockInfo));
    Map<Long, Set<LineageFile>> workerToCheckpointFile = Maps.newHashMap();
    workerToCheckpointFile.put(workerId, checkpointFiles);
    Whitebox.setInternalState(mLineageMaster, "mWorkerToCheckpointFile", workerToCheckpointFile);

    LineageCommand command =
        mLineageMaster.lineageWorkerHeartbeat(workerId, Lists.newArrayList(fileId));
    Assert.assertEquals(CommandType.Persist, command.commandType);
    Assert.assertEquals(1, command.checkpointFiles.size());
    Assert.assertEquals(fileId, command.checkpointFiles.get(0).fileId);
    Assert.assertEquals(blockId, (long) command.checkpointFiles.get(0).blockIds.get(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queueForCheckpointTest() throws Exception {
    long workerId = 1;
    long fileId = 0;
    long lineageId = mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.blockInfo = new BlockInfo();
    BlockLocation blockLocation = new BlockLocation();
    blockLocation.workerId = workerId;
    fileBlockInfo.blockInfo.locations = Lists.newArrayList(blockLocation);
    Mockito.when(mFileSystemMaster.getFileBlockInfoList(fileId))
        .thenReturn(Lists.newArrayList(fileBlockInfo));

    CheckpointPlan plan = new CheckpointPlan(Lists.newArrayList(lineageId));
    mLineageMaster.queueForCheckpoint(plan);
    Map<Long, Set<LineageFile>> workerToCheckpointFile = (Map<Long, Set<LineageFile>>) Whitebox
        .getInternalState(mLineageMaster, "mWorkerToCheckpointFile");
    Assert.assertTrue(workerToCheckpointFile.containsKey(workerId));
    Assert.assertEquals(fileId, workerToCheckpointFile.get(workerId).iterator().next().getFileId());
  }
}
