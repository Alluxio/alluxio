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

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.job.JobMasterContext;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

/**
 * Tests {@link PersistDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class, JobMasterContext.class})
public final class PersistDefinitionTest {
  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private JobMasterContext mMockJobMasterContext;

  @Before
  public void before() {
    mMockJobMasterContext = Mockito.mock(JobMasterContext.class);
    mMockFileSystem = Mockito.mock(FileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);
  }

  @Test
  public void selectExecutorsTest() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    PersistConfig config = new PersistConfig(uri.getPath(), -1, true, "");

    WorkerNetAddress workerNetAddress = new WorkerNetAddress().setDataPort(10);
    WorkerInfo workerInfo = new WorkerInfo().setAddress(workerNetAddress);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    BlockLocation location = new BlockLocation();
    location.setWorkerAddress(workerNetAddress);
    blockInfo.setLocations(Lists.newArrayList(location));
    FileInfo testFileInfo = new FileInfo();
    testFileInfo.setFileBlockInfos(Lists.newArrayList(fileBlockInfo));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));

    Map<WorkerInfo, SerializableVoid> result =
        new PersistDefinition(mMockFileSystemContext, mMockFileSystem).selectExecutors(config,
            Lists.newArrayList(workerInfo), mMockJobMasterContext);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo, result.keySet().iterator().next());
  }

  @Test
  public void selectExecutorsMissingLocationTest() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    PersistConfig config = new PersistConfig(uri.getPath(), -1, true, "");

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    FileInfo testFileInfo = new FileInfo();
    testFileInfo.setFileBlockInfos(Lists.newArrayList(fileBlockInfo));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));

    try {
      new PersistDefinition(mMockFileSystemContext, mMockFileSystem).selectExecutors(config,
          Lists.newArrayList(new WorkerInfo()), mMockJobMasterContext);
    } catch (Exception e) {
      Assert.assertEquals("Block " + blockId + " does not exist", e.getMessage());
    }
  }
}
