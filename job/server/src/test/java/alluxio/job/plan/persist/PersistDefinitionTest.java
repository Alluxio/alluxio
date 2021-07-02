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

package alluxio.job.plan.persist;

import static org.mockito.Mockito.mock;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.underfs.UfsManager;
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

import java.util.Set;

/**
 * Tests {@link PersistDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class, JobServerContext.class})
public final class PersistDefinitionTest {
  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private JobServerContext mJobServerContext;

  @Before
  public void before() {
    mMockFileSystem = Mockito.mock(FileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);
    mJobServerContext =
        new JobServerContext(mMockFileSystem, mMockFileSystemContext, mock(UfsManager.class));
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

    Set<Pair<WorkerInfo, SerializableVoid>> result =
        new PersistDefinition().selectExecutors(config,
            Lists.newArrayList(workerInfo), new SelectExecutorsContext(1, mJobServerContext));
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo, result.iterator().next().getFirst());
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
      new PersistDefinition().selectExecutors(config,
          Lists.newArrayList(new WorkerInfo()), new SelectExecutorsContext(1, mJobServerContext));
    } catch (Exception e) {
      Assert.assertEquals("Block " + blockId + " does not exist", e.getMessage());
    }
  }
}
