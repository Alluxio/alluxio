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

package alluxio.job.replicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.TestBlockInStream;
import alluxio.client.block.stream.TestBlockOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.job.JobServerContext;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.underfs.UfsManager;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link ReplicateConfig}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class, JobServerContext.class})
public final class ReplicateDefinitionTest {
  private static final long TEST_BLOCK_ID = 1L;
  private static final long TEST_BLOCK_SIZE = 512L;
  private static final int MAX_BYTES = 1000;
  private static final WorkerNetAddress ADDRESS_1 =
      new WorkerNetAddress().setHost("host1").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_2 =
      new WorkerNetAddress().setHost("host2").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_3 =
      new WorkerNetAddress().setHost("host3").setDataPort(10);
  private static final WorkerNetAddress LOCAL_ADDRESS =
      new WorkerNetAddress().setHost(NetworkAddressUtils
          .getLocalHostName((int) ServerConfiguration
              .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS))).setDataPort(10);
  private static final WorkerInfo WORKER_INFO_1 = new WorkerInfo().setAddress(ADDRESS_1);
  private static final WorkerInfo WORKER_INFO_2 = new WorkerInfo().setAddress(ADDRESS_2);
  private static final WorkerInfo WORKER_INFO_3 = new WorkerInfo().setAddress(ADDRESS_3);

  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private FileSystem mMockFileSystem;
  private JobServerContext mMockJobServerContext;
  private UfsManager mMockUfsManager;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    when(mMockFileSystemContext.getClientContext())
        .thenReturn(ClientContext.create(ServerConfiguration.global()));
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mMockFileSystem = mock(FileSystem.class);
    mMockUfsManager = mock(UfsManager.class);
    mMockJobServerContext =
        new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager);
  }

  /**
   * Helper function to select executors.
   *
   * @param blockLocations where the block is store currently
   * @param numReplicas how many replicas to replicate or evict
   * @param workerInfoList a list of current available job workers
   * @return the selection result
   */
  private Map<WorkerInfo, SerializableVoid> selectExecutorsTestHelper(
      List<BlockLocation> blockLocations, int numReplicas, List<WorkerInfo> workerInfoList)
      throws Exception {
    BlockInfo blockInfo = new BlockInfo().setBlockId(TEST_BLOCK_ID);
    blockInfo.setLocations(blockLocations);
    when(mMockBlockStore.getInfo(TEST_BLOCK_ID)).thenReturn(blockInfo);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);

    String path = "/test";
    ReplicateConfig config = new ReplicateConfig(path, TEST_BLOCK_ID, numReplicas);
    ReplicateDefinition definition = new ReplicateDefinition();
    return definition.selectExecutors(config, workerInfoList,
        new SelectExecutorsContext(1, mMockJobServerContext));
  }

  /**
   * Helper function to run a replicate task.
   *
   * @param blockWorkers available block workers
   * @param mockInStream mock blockInStream returned by the Block Store
   * @param mockOutStream mock blockOutStream returned by the Block Store
   */
  private void runTaskReplicateTestHelper(List<BlockWorkerInfo> blockWorkers,
      BlockInStream mockInStream, BlockOutStream mockOutStream) throws Exception {
    String path = "/test";
    URIStatus status = new URIStatus(
        new FileInfo().setPath(path).setBlockIds(Lists.newArrayList(TEST_BLOCK_ID))
            .setFileBlockInfos(Lists.newArrayList(
                new FileBlockInfo().setBlockInfo(new BlockInfo().setBlockId(TEST_BLOCK_ID)))));
    when(mMockFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(status);

    when(mMockBlockStore.getAllWorkers()).thenReturn(blockWorkers);
    when(mMockBlockStore.getInStream(anyLong(),
            any(InStreamOptions.class))).thenReturn(mockInStream);
    when(
        mMockBlockStore.getOutStream(eq(TEST_BLOCK_ID), eq(-1L), eq(LOCAL_ADDRESS),
            any(OutStreamOptions.class))).thenReturn(mockOutStream);
    when(mMockBlockStore.getInfo(TEST_BLOCK_ID))
        .thenReturn(new BlockInfo().setBlockId(TEST_BLOCK_ID)
            .setLocations(Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1))));
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);

    ReplicateConfig config = new ReplicateConfig(path, TEST_BLOCK_ID, 1 /* value not used */);
    ReplicateDefinition definition = new ReplicateDefinition();
    definition.runTask(config, null, new RunTaskContext(1, 1, mMockJobServerContext));
  }

  @Test
  public void selectExecutorsOnlyOneWorkerAvailable() throws Exception {
    Map<WorkerInfo, SerializableVoid> result =
        selectExecutorsTestHelper(Lists.<BlockLocation>newArrayList(), 1,
            Lists.newArrayList(WORKER_INFO_1));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_1, null);
    // select the only worker
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsOnlyOneWorkerValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_2, null);
    // select one worker left
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsTwoWorkersValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 2,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_2, null);
    expected.put(WORKER_INFO_3, null);
    // select both workers left
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsOneOutOFTwoWorkersValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // select one worker out of two
    assertEquals(1, result.size());
    assertEquals(null, result.values().iterator().next());
  }

  @Test
  public void selectExecutorsNoWorkerValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    // select none as no choice left
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsInsufficientWorkerValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 2,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_2, null);
    // select the only worker left though more copies are requested
    assertEquals(expected, result);
  }

  @Test
  public void runTaskNoBlockWorker() throws Exception {
    byte[] input = BufferUtils.getIncreasingByteArray(0, (int) TEST_BLOCK_SIZE);

    TestBlockInStream mockInStream =
        new TestBlockInStream(input, TEST_BLOCK_ID, input.length, false, BlockInStreamSource.LOCAL);
    TestBlockOutStream mockOutStream =
        new TestBlockOutStream(ByteBuffer.allocate(MAX_BYTES), TEST_BLOCK_SIZE);
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.NO_LOCAL_BLOCK_WORKER_REPLICATE_TASK
        .getMessage(TEST_BLOCK_ID));
    runTaskReplicateTestHelper(Lists.<BlockWorkerInfo>newArrayList(), mockInStream, mockOutStream);
  }

  @Test
  public void runTaskLocalBlockWorker() throws Exception {
    byte[] input = BufferUtils.getIncreasingByteArray(0, (int) TEST_BLOCK_SIZE);

    TestBlockInStream mockInStream =
        new TestBlockInStream(input, TEST_BLOCK_ID, input.length, false, BlockInStreamSource.LOCAL);
    TestBlockOutStream mockOutStream =
        new TestBlockOutStream(ByteBuffer.allocate(MAX_BYTES), TEST_BLOCK_SIZE);
    BlockWorkerInfo localBlockWorker = new BlockWorkerInfo(LOCAL_ADDRESS, TEST_BLOCK_SIZE, 0);
    runTaskReplicateTestHelper(Lists.newArrayList(localBlockWorker), mockInStream, mockOutStream);
    assertTrue(Arrays.equals(input, mockOutStream.getWrittenData()));
  }

  @Test
  public void runTaskInputIOException() throws Exception {
    BlockInStream mockInStream = mock(BlockInStream.class);
    BlockOutStream mockOutStream = mock(BlockOutStream.class);

    BlockWorkerInfo localBlockWorker = new BlockWorkerInfo(LOCAL_ADDRESS, TEST_BLOCK_SIZE, 0);
    doThrow(new IOException("test")).when(mockInStream).read(any(byte[].class), anyInt(), anyInt());
    doThrow(new IOException("test")).when(mockInStream).read(any(byte[].class));
    try {
      runTaskReplicateTestHelper(Lists.newArrayList(localBlockWorker), mockInStream, mockOutStream);
      fail("Expected the task to throw and IOException");
    } catch (IOException e) {
      assertEquals("test", e.getMessage());
    }
    verify(mockOutStream).cancel();
  }
}
