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

package alluxio.job.load;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.load.LoadDefinition.LoadTask;
import alluxio.underfs.UfsManager;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link LoadDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, JobServerContext.class, FileSystemContext.class,
    AlluxioBlockStore.class})
public class LoadDefinitionTest {
  private static final String TEST_URI = "/test";

  private static final List<WorkerInfo> JOB_WORKERS = new ImmutableList.Builder<WorkerInfo>()
      .add(new WorkerInfo().setId(0).setAddress(new WorkerNetAddress().setHost("host0")))
      .add(new WorkerInfo().setId(1).setAddress(new WorkerNetAddress().setHost("host1")))
      .add(new WorkerInfo().setId(2).setAddress(new WorkerNetAddress().setHost("host2")))
      .add(new WorkerInfo().setId(3).setAddress(new WorkerNetAddress().setHost("host3"))).build();

  private static final List<BlockWorkerInfo> BLOCK_WORKERS =
      new ImmutableList.Builder<BlockWorkerInfo>()
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host1"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host2"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host3"), 0, 0)).build();

  private JobServerContext mJobServerContext;
  private FileSystem mMockFileSystem;
  private AlluxioBlockStore mMockBlockStore;
  private FileSystemContext mMockFsContext;

  @Before
  public void before() throws Exception {
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mMockFsContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(Mockito.any(FileSystemContext.class)))
        .thenReturn(mMockBlockStore);
    Mockito.when(mMockBlockStore.getAllWorkers()).thenReturn(BLOCK_WORKERS);
    PowerMockito.when(mMockFsContext.getClientContext())
        .thenReturn(ClientContext.create(ServerConfiguration.global()));
    PowerMockito.when(mMockFsContext.getConf())
        .thenReturn(ServerConfiguration.global());
    mJobServerContext = new JobServerContext(mMockFileSystem, mMockFsContext,
        Mockito.mock(UfsManager.class));
  }

  @Test
  public void replicationSatisfied() throws Exception {
    int numBlocks = 7;
    int replication = 3;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, replication);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition().selectExecutors(config,
            JOB_WORKERS, new SelectExecutorsContext(1, mJobServerContext));
    // Check that we are loading the right number of blocks.
    int totalBlockLoads = 0;
    for (List<LoadTask> blocks : assignments.values()) {
      totalBlockLoads += blocks.size();
    }
    Assert.assertEquals(numBlocks * replication, totalBlockLoads);
  }

  @Test
  public void skipJobWorkersWithoutLocalBlockWorkers() throws Exception {
    List<BlockWorkerInfo> blockWorkers =
        Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0));
    Mockito.when(mMockBlockStore.getAllWorkers()).thenReturn(blockWorkers);
    createFileWithNoLocations(TEST_URI, 10);
    LoadConfig config = new LoadConfig(TEST_URI, 1);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition().selectExecutors(config, JOB_WORKERS,
            new SelectExecutorsContext(1, mJobServerContext));
    Assert.assertEquals(1, assignments.size());
    Assert.assertEquals(10, assignments.values().iterator().next().size());
  }

  @Test
  public void notEnoughWorkersForReplication() throws Exception {
    createFileWithNoLocations(TEST_URI, 1);
    LoadConfig config = new LoadConfig(TEST_URI, 5); // set replication to 5
    try {
      new LoadDefinition().selectExecutors(config, JOB_WORKERS,
          new SelectExecutorsContext(1, mJobServerContext));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertThat(e.getMessage(), CoreMatchers.containsString(
          "Failed to find enough block workers to replicate to. Needed 5 but only found 4."));
    }
  }

  @Test
  public void notEnoughJobWorkersWithLocalBlockWorkers() throws Exception {
    List<BlockWorkerInfo> blockWorkers =
        Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0),
            new BlockWorkerInfo(new WorkerNetAddress().setHost("otherhost"), 0, 0));
    Mockito.when(mMockBlockStore.getAllWorkers()).thenReturn(blockWorkers);
    createFileWithNoLocations(TEST_URI, 1);
    LoadConfig config = new LoadConfig(TEST_URI, 2); // set replication to 2
    try {
      new LoadDefinition().selectExecutors(config,
          JOB_WORKERS, new SelectExecutorsContext(1, mJobServerContext));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertThat(e.getMessage(),
          CoreMatchers.containsString("Available workers without the block: [host0]"));
      Assert.assertThat(e.getMessage(),
          CoreMatchers.containsString("The following workers could not be used because "
              + "they have no local job workers: [otherhost]"));
    }
  }

  private FileInfo createFileWithNoLocations(String testFile, int numOfBlocks) throws Exception {
    FileInfo testFileInfo = new FileInfo();
    AlluxioURI uri = new AlluxioURI(testFile);
    List<FileBlockInfo> blockInfos = Lists.newArrayList();
    for (int i = 0; i < numOfBlocks; i++) {
      blockInfos.add(new FileBlockInfo()
          .setBlockInfo(new BlockInfo().setLocations(Lists.<BlockLocation>newArrayList())));
    }
    testFileInfo.setFolder(false).setPath(testFile).setFileBlockInfos(blockInfos);
    Mockito.when(mMockFileSystem.listStatus(uri))
        .thenReturn(Lists.newArrayList(new URIStatus(testFileInfo)));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));
    return testFileInfo;
  }
}
