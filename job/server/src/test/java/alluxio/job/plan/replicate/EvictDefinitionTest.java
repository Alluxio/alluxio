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

package alluxio.job.plan.replicate;

import static org.mockito.Mockito.when;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.underfs.UfsManager;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Set;

/**
 * Tests {@link EvictDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class, JobServerContext.class})
public final class EvictDefinitionTest {
  private static final long TEST_BLOCK_ID = 1L;
  private static final WorkerNetAddress ADDRESS_1 =
      new WorkerNetAddress().setHost("host1").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_2 =
      new WorkerNetAddress().setHost("host2").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_3 =
      new WorkerNetAddress().setHost("host3").setDataPort(10);
  private static final WorkerInfo WORKER_INFO_1 = new WorkerInfo().setAddress(ADDRESS_1);
  private static final WorkerInfo WORKER_INFO_2 = new WorkerInfo().setAddress(ADDRESS_2);
  private static final WorkerInfo WORKER_INFO_3 = new WorkerInfo().setAddress(ADDRESS_3);
  private static final Set<Pair<WorkerInfo, SerializableVoid>> EMPTY = Sets.newHashSet();

  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private JobServerContext mJobServerContext;

  @Before
  public void before() {
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mJobServerContext = new JobServerContext(mMockFileSystem, mMockFileSystemContext,
        PowerMockito.mock(UfsManager.class));
  }

  /**
   * Helper function to select executors.
   *
   * @param blockLocations where the block is stored currently
   * @param replicas how many replicas to evict
   * @param workerInfoList a list of currently available job workers
   * @return the selection result
   */
  private Set<Pair<WorkerInfo, SerializableVoid>> selectExecutorsTestHelper(
      List<BlockLocation> blockLocations, int replicas, List<WorkerInfo> workerInfoList)
      throws Exception {
    BlockInfo blockInfo = new BlockInfo().setBlockId(TEST_BLOCK_ID);
    blockInfo.setLocations(blockLocations);
    when(mMockBlockStore.getInfo(TEST_BLOCK_ID)).thenReturn(blockInfo);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);

    EvictConfig config = new EvictConfig("", TEST_BLOCK_ID, replicas);
    EvictDefinition definition = new EvictDefinition();
    return definition.selectExecutors(config, workerInfoList,
        new SelectExecutorsContext(1, mJobServerContext));
  }

  @Test
  public void selectExecutorsNoBlockWorkerHasBlock() throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> result =
        selectExecutorsTestHelper(Lists.<BlockLocation>newArrayList(), 1,
            Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // Expect none as no worker has this copy
    Assert.assertEquals(EMPTY, result);
  }

  @Test
  public void selectExecutorsNoJobWorkerHasBlock() throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_2, WORKER_INFO_3));
    // Expect none as no worker that is available has this copy
    Assert.assertEquals(EMPTY, result);
  }

  @Test
  public void selectExecutorsOnlyOneBlockWorkerHasBlock() throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_1, null));
    // Expect the only worker 1 having this block
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsAnyOneWorkers() throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(Lists
            .newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1),
                new BlockLocation().setWorkerAddress(ADDRESS_2),
                new BlockLocation().setWorkerAddress(ADDRESS_3)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // Expect one worker from all workers having this block
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(null, result.iterator().next().getSecond());
  }

  @Test
  public void selectExecutorsAllWorkers() throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(Lists
            .newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1),
                new BlockLocation().setWorkerAddress(ADDRESS_2),
                new BlockLocation().setWorkerAddress(ADDRESS_3)), 3,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_1, null));
    expected.add(new Pair<>(WORKER_INFO_2, null));
    expected.add(new Pair<>(WORKER_INFO_3, null));
    // Expect all workers are selected as they all have this block
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsBothWorkers() throws Exception {
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(Lists
            .newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1),
                new BlockLocation().setWorkerAddress(ADDRESS_2)), 3,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_1, null));
    expected.add(new Pair<>(WORKER_INFO_2, null));
    // Expect both workers having this block should be selected
    Assert.assertEquals(expected, result);
  }
}
