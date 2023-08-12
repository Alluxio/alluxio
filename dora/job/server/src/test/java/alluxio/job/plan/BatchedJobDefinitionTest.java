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

package alluxio.job.plan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.batch.BatchedJobDefinition;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.underfs.UfsManager;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link BatchedJobDefinition}.
 */
public class BatchedJobDefinitionTest {
  private static final String TEST_URI = "/test";
  private static final WorkerNetAddress WORKER_ADDR_0 =
      new WorkerNetAddress().setHost("host0")
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack1"))));
  private static final WorkerNetAddress WORKER_ADDR_1 =
      new WorkerNetAddress().setHost("host1")
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack1"))));
  private static final WorkerNetAddress WORKER_ADDR_2 =
      new WorkerNetAddress().setHost("host2")
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack2"))));
  private static final WorkerNetAddress WORKER_ADDR_3 =
      new WorkerNetAddress().setHost("host3")
          .setTieredIdentity(
              new TieredIdentity(Collections.singletonList(
                  new TieredIdentity.LocalityTier("rack", "rack2"))));

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
  private BlockStoreClient mMockBlockStore;
  private FileSystemContext mMockFsContext;

  @Before
  public void before() throws Exception {
    mMockFileSystem = mock(FileSystem.class);
    mMockFsContext = mock(FileSystemContext.class);
    when(mMockFsContext.getCachedWorkers()).thenReturn(BLOCK_WORKERS);
    when(mMockFsContext.getClientContext())
        .thenReturn(ClientContext.create(Configuration.global()));
    when(mMockFsContext.getClusterConf()).thenReturn(Configuration.global());
    mJobServerContext = new JobServerContext(mMockFileSystem, mMockFsContext,
        mock(UfsManager.class));
  }

  @Test
  public void batchPersist() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    PersistConfig config = new PersistConfig(uri.getPath(), -1, true, "");
    HashSet<Map<String, String>> configs = Sets.newHashSet();
    ObjectMapper oMapper = new ObjectMapper();
    Map<String, String> map = oMapper.convertValue(config, Map.class);
    configs.add(map);
    BatchedJobConfig batchedJobConfig = new BatchedJobConfig("Persist", configs);
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
    when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));

    Set<Pair<WorkerInfo, BatchedJobDefinition.BatchedJobTask>> result =
        new BatchedJobDefinition().selectExecutors(batchedJobConfig, Lists.newArrayList(workerInfo),
            new SelectExecutorsContext(1, mJobServerContext));
    Assert.assertNull(result.iterator().next().getSecond().getJobTaskArgs());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo, result.iterator().next().getFirst());
  }
}
