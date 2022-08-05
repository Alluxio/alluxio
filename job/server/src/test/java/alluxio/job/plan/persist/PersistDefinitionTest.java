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
import alluxio.client.block.BlockWorkerInfo;
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
import org.mockito.Mockito;

import java.util.Set;

/**
 * Tests {@link PersistDefinition}.
 */
public final class PersistDefinitionTest {
  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private JobServerContext mJobServerContext;

  @Before
  public void before() {
    mMockFileSystem = mock(FileSystem.class);
    mMockFileSystemContext = mock(FileSystemContext.class);
    mJobServerContext =
        new JobServerContext(mMockFileSystem, mMockFileSystemContext, mock(UfsManager.class));
  }

  /**
   * Select executor which has the most blocks.
   */
  @Test
  public void selectExecutorsTest() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    PersistConfig config = new PersistConfig(uri.getPath(), -1, true, "");

    WorkerNetAddress workerNetAddress1 = WorkerNetAddress.newBuilder("host1", 10).build();
    WorkerNetAddress workerNetAddress2 = WorkerNetAddress.newBuilder("host2", 100).build();

    BlockWorkerInfo blockWorkerInfo1 = new BlockWorkerInfo(workerNetAddress1, 1, 1);
    BlockWorkerInfo blockWorkerInfo2 = new BlockWorkerInfo(workerNetAddress2, 1, 1);

    WorkerInfo workerInfo1 = new WorkerInfo().setAddress(workerNetAddress1);
    WorkerInfo workerInfo2 = new WorkerInfo().setAddress(workerNetAddress2);

    FileBlockInfo fileBlockInfo1 = mockFileBlockInfo(1, workerNetAddress2);
    FileBlockInfo fileBlockInfo2 = mockFileBlockInfo(2, workerNetAddress1);
    FileBlockInfo fileBlockInfo3 = mockFileBlockInfo(3, workerNetAddress1);
    FileInfo testFileInfo = new FileInfo();
    testFileInfo.setFileBlockInfos(
        Lists.newArrayList(fileBlockInfo1, fileBlockInfo2, fileBlockInfo3));

    Mockito.when(mMockFileSystemContext.getCachedWorkers()).thenReturn(
        Lists.newArrayList(blockWorkerInfo1, blockWorkerInfo2));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));

    Set<Pair<WorkerInfo, SerializableVoid>> result = new PersistDefinition()
        .selectExecutors(config, Lists.newArrayList(workerInfo2, workerInfo1),
            new SelectExecutorsContext(1, mJobServerContext));
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo1, result.iterator().next().getFirst());
  }

  private FileBlockInfo mockFileBlockInfo(long blockId, WorkerNetAddress workerNetAddress) {
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    BlockLocation location = new BlockLocation();
    location.setWorkerAddress(workerNetAddress);
    blockInfo.setLocations(Lists.newArrayList(location));
    return fileBlockInfo;
  }
}
