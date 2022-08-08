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

package alluxio.job.plan.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.load.LoadDefinition.LoadTask;
import alluxio.underfs.UfsManager;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests {@link LoadDefinition}.
 */
public class LoadDefinitionTest {
  private static final String TEST_URI = "/test";
  private static final WorkerNetAddress WORKER_ADDR_0 =
      WorkerNetAddress.newBuilder("host0", 1)
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack1")))).build();
  private static final WorkerNetAddress WORKER_ADDR_1 =
      WorkerNetAddress.newBuilder("host1", 2)
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack1")))).build();
  private static final WorkerNetAddress WORKER_ADDR_2 =
      WorkerNetAddress.newBuilder("host2", 3)
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack2")))).build();
  private static final WorkerNetAddress WORKER_ADDR_3 =
      WorkerNetAddress.newBuilder("host3", 4)
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack2")))).build();

  private static final List<WorkerInfo> JOB_WORKERS = new ImmutableList.Builder<WorkerInfo>()
      .add(new WorkerInfo().setId(0).setAddress(WORKER_ADDR_0))
      .add(new WorkerInfo().setId(1).setAddress(WORKER_ADDR_1))
      .add(new WorkerInfo().setId(2).setAddress(WORKER_ADDR_2))
      .add(new WorkerInfo().setId(3).setAddress(WORKER_ADDR_3))
      .build();

  private static final List<BlockWorkerInfo> BLOCK_WORKERS =
      new ImmutableList.Builder<BlockWorkerInfo>()
          .add(new BlockWorkerInfo(WORKER_ADDR_0, 0, 0))
          .add(new BlockWorkerInfo(WORKER_ADDR_1, 0, 0))
          .add(new BlockWorkerInfo(WORKER_ADDR_2, 0, 0))
          .add(new BlockWorkerInfo(WORKER_ADDR_3, 0, 0)).build();

  private JobServerContext mJobServerContext;
  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFsContext;

  @Before
  public void before() throws Exception {
    mMockFileSystem = mock(FileSystem.class);
    mMockFsContext = mock(FileSystemContext.class);
    when(mMockFsContext.getCachedWorkers()).thenReturn(BLOCK_WORKERS);
    when(mMockFsContext.getClientContext())
        .thenReturn(ClientContext.create(Configuration.global()));
    when(mMockFsContext.getClusterConf()).thenReturn(Configuration.global());
    when(mMockFsContext.getPathConf(any(AlluxioURI.class)))
        .thenReturn(Configuration.global());
    mJobServerContext =
        new JobServerContext(mMockFileSystem, mMockFsContext, mock(UfsManager.class));
  }

  @Test
  public void replicationSatisfied() throws Exception {
    int numBlocks = 7;
    int replication = 3;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, replication, Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, false);
    Set<Pair<WorkerInfo, ArrayList<LoadTask>>> assignments = new LoadDefinition()
        .selectExecutors(config, JOB_WORKERS, new SelectExecutorsContext(1, mJobServerContext));
    // Check that we are loading the right number of blocks.
    int totalBlockLoads = 0;

    for (Pair<WorkerInfo, ArrayList<LoadTask>> assignment : assignments) {
      totalBlockLoads += assignment.getSecond().size();
    }
    Assert.assertEquals(numBlocks * replication, totalBlockLoads);
  }

  @Test
  public void skipJobWorkersWithoutLocalBlockWorkers() throws Exception {
    List<BlockWorkerInfo> blockWorkers = Arrays.asList(new BlockWorkerInfo(
        WorkerNetAddress.newBuilder("host0", 1).build(), 0, 0));
    when(mMockFsContext.getCachedWorkers()).thenReturn(blockWorkers);
    createFileWithNoLocations(TEST_URI, 10);
    LoadConfig config = new LoadConfig(TEST_URI, 1, Collections.EMPTY_SET, Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, false);
    Set<Pair<WorkerInfo, ArrayList<LoadTask>>> assignments = new LoadDefinition()
        .selectExecutors(config, JOB_WORKERS, new SelectExecutorsContext(1, mJobServerContext));
    Assert.assertEquals(10, assignments.size());
    Assert.assertEquals(1, assignments.iterator().next().getSecond().size());
  }

  @Test
  public void notEnoughWorkersForReplication() throws Exception {
    createFileWithNoLocations(TEST_URI, 1);
    LoadConfig config = new LoadConfig(TEST_URI, 5, Collections.EMPTY_SET, Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, false); // set replication to 5
    IOException exception = Assert.assertThrows(
        IOException.class, () -> new LoadDefinition().selectExecutors(
            config, JOB_WORKERS, new SelectExecutorsContext(1, mJobServerContext)));
    Assert.assertTrue(exception.getMessage().contains(
        "Failed to find enough block workers to replicate to. Needed 5 but only found 4."));
  }

  @Test
  public void notEnoughJobWorkersWithLocalBlockWorkers() throws Exception {
    List<BlockWorkerInfo> blockWorkers = Arrays.asList(new BlockWorkerInfo(
        WorkerNetAddress.newBuilder("host0", 1).build(), 0, 0),
        new BlockWorkerInfo(WorkerNetAddress.newBuilder("otherhost", 1).build(), 0, 0));
    when(mMockFsContext.getCachedWorkers()).thenReturn(blockWorkers);
    createFileWithNoLocations(TEST_URI, 1);
    LoadConfig config = new LoadConfig(TEST_URI, 2, Collections.EMPTY_SET, Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, false); // set replication to 2
    IOException exception = Assert.assertThrows(IOException.class,
        () -> new LoadDefinition().selectExecutors(config, JOB_WORKERS,
            new SelectExecutorsContext(1, mJobServerContext)));
    Assert.assertTrue(exception.getMessage().contains("Available workers without the block"));
    Assert.assertTrue(exception.getMessage().contains(
        "The following workers could not be used"
            + " because they have no local job workers: [otherhost]"));
  }

  @Test
  public void loadedBySpecifiedLocalityIdentity() throws Exception {
    Set<Long> workerIds = new HashSet<>();
    workerIds.add(0L);
    workerIds.add(1L);
    loadedBySpecifiedHost(Collections.EMPTY_SET, Collections.EMPTY_SET,
        Collections.singleton("RACK1"), Collections.EMPTY_SET, workerIds);
  }

  @Test
  public void loadedBySpecifiedLocalityIdentity2() throws Exception {
    Set<Long> workerIds = new HashSet<>();
    workerIds.add(2L);
    workerIds.add(3L);
    loadedBySpecifiedHost(Collections.EMPTY_SET, Collections.EMPTY_SET,
        Collections.singleton("RACK2"), Collections.EMPTY_SET, workerIds);
  }

  @Test
  public void loadedByNotExcludedLocalityIdentity() throws Exception {
    Set<Long> workerIds = new HashSet<>();
    workerIds.add(2L);
    workerIds.add(3L);
    Set<String> localityIdentities = new HashSet<>();
    localityIdentities.add("RACK2");
    localityIdentities.add("RACK3");
    loadedBySpecifiedHost(Collections.EMPTY_SET, Collections.EMPTY_SET, localityIdentities,
        Collections.singleton("RACK3"), workerIds);
  }

  @Test
  public void loadedBySpecifiedWorker() throws Exception {
    Set<Long> workerIds = new HashSet<>();
    workerIds.add(2L);
    loadedBySpecifiedHost(Collections.singleton("HOST2"), Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, workerIds);
  }

  @Test
  public void loadedBySpecifiedWorker2() throws Exception {
    Set<Long> workerIds = new HashSet<>();
    workerIds.add(3L);
    loadedBySpecifiedHost(Collections.singleton("HOST3"), Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, workerIds);
  }

  @Test
  public void loadedByNotExcludedWorker() throws Exception {
    Set<Long> workerIds = new HashSet<>();
    workerIds.add(2L);
    workerIds.add(3L);
    Set<String> workerSet = new HashSet<>();
    workerSet.add("HOST2");
    workerSet.add("HOST3");
    loadedBySpecifiedHost(workerSet, Collections.singleton("HOST3"), Collections.EMPTY_SET,
        Collections.EMPTY_SET, workerIds);
  }

  private void loadedBySpecifiedHost(Set<String> workerSet, Set<String> excludedWorkerSet,
      Set<String> localityIds, Set<String> excludedLocalityIds, Set<Long> workerIds)
      throws Exception {
    int numBlocks = 10;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, 1, workerSet, excludedWorkerSet, localityIds,
        excludedLocalityIds, false);
    Set<Pair<WorkerInfo, ArrayList<LoadTask>>> assignments = new LoadDefinition()
        .selectExecutors(config, JOB_WORKERS, new SelectExecutorsContext(1, mJobServerContext));
    // Check that we are loading the right number of blocks.
    int totalBlockLoads = 0;

    for (Pair<WorkerInfo, ArrayList<LoadTask>> assignment : assignments) {
      totalBlockLoads += assignment.getSecond().size();
      Assert.assertTrue(workerIds.contains(assignment.getFirst().getId()));
    }
    Assert.assertEquals(numBlocks, totalBlockLoads);
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
    when(mMockFileSystem.listStatus(uri))
        .thenReturn(Lists.newArrayList(new URIStatus(testFileInfo)));
    when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));
    return testFileInfo;
  }
}
