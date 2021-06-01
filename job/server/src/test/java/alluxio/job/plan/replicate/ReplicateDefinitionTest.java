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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
import alluxio.Constants;
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
import alluxio.collections.Pair;
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link ReplicateConfig}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, FileSystemContext.class, JobServerContext.class,
    BlockInStream.class})
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
  private static final String TEST_PATH = "/test";

  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private FileSystem mMockFileSystem;
  private JobServerContext mMockJobServerContext;
  private UfsManager mMockUfsManager;
  private BlockInfo mTestBlockInfo;
  private URIStatus mTestStatus;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    when(mMockFileSystemContext.getClientContext())
        .thenReturn(ClientContext.create(ServerConfiguration.global()));
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mMockFileSystem = mock(FileSystem.class);
    mMockUfsManager = mock(UfsManager.class);
    mMockJobServerContext =
        new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);
    mTestBlockInfo = new BlockInfo().setBlockId(TEST_BLOCK_ID).setLength(TEST_BLOCK_SIZE);
    when(mMockBlockStore.getInfo(TEST_BLOCK_ID)).thenReturn(mTestBlockInfo);
    mTestStatus = new URIStatus(
        new FileInfo().setPath(TEST_PATH).setBlockIds(Lists.newArrayList(TEST_BLOCK_ID))
            .setPersisted(true)
            .setFileBlockInfos(Lists.newArrayList(
                new FileBlockInfo().setBlockInfo(mTestBlockInfo))));
  }

  /**
   * Helper function to select executors.
   *
   * @param numReplicas how many replicas to replicate or evict
   * @param workerInfoList a list of current available job workers
   * @return the selection result
   */
  private Set<Pair<WorkerInfo, SerializableVoid>> selectExecutorsTestHelper(
      int numReplicas, List<WorkerInfo> workerInfoList)
      throws Exception {
    ReplicateConfig config = new ReplicateConfig(TEST_PATH, TEST_BLOCK_ID, numReplicas);
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
    when(mMockFileSystem.getStatus(any(AlluxioURI.class))).thenReturn(mTestStatus);
    when(mMockFileSystemContext.getCachedWorkers()).thenReturn(blockWorkers);
    when(mMockBlockStore.getInStream(anyLong(),
            any(InStreamOptions.class))).thenReturn(mockInStream);
    when(mMockBlockStore.getInStream(any(BlockInfo.class),
        any(InStreamOptions.class), any(Map.class))).thenReturn(mockInStream);
    PowerMockito.mockStatic(BlockInStream.class);
    when(BlockInStream.create(any(FileSystemContext.class), any(BlockInfo.class),
        any(WorkerNetAddress.class), any(BlockInStreamSource.class), any(InStreamOptions.class)))
        .thenReturn(mockInStream);
    when(
        mMockBlockStore.getOutStream(eq(TEST_BLOCK_ID), eq(TEST_BLOCK_SIZE), eq(LOCAL_ADDRESS),
            any(OutStreamOptions.class))).thenReturn(mockOutStream);
    when(mMockBlockStore.getInfo(TEST_BLOCK_ID))
        .thenReturn(mTestBlockInfo
            .setLocations(Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1))));
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    when(AlluxioBlockStore.create(mMockFileSystemContext)).thenReturn(mMockBlockStore);

    ReplicateConfig config = new ReplicateConfig(TEST_PATH, TEST_BLOCK_ID, 1 /* value not used */);
    ReplicateDefinition definition = new ReplicateDefinition();
    definition.runTask(config, null, new RunTaskContext(1, 1, mMockJobServerContext));
  }

  @Test
  public void selectExecutorsOnlyOneWorkerAvailable() throws Exception {
    mTestBlockInfo.setLocations(Lists.newArrayList());
    Set<Pair<WorkerInfo, SerializableVoid>> result =
        selectExecutorsTestHelper(1,
            Lists.newArrayList(WORKER_INFO_1));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_1, null));
    // select the only worker
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsOnlyOneWorkerValid() throws Exception {
    mTestBlockInfo.setLocations(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)));
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_2, null));
    // select one worker left
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsTwoWorkersValid() throws Exception {
    mTestBlockInfo.setLocations(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)));
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        2,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_2, null));
    expected.add(new Pair<>(WORKER_INFO_3, null));
    // select both workers left
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsOneOutOFTwoWorkersValid() throws Exception {
    mTestBlockInfo.setLocations(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)));
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // select one worker out of two
    assertEquals(1, result.size());
    assertEquals(null, result.iterator().next().getSecond());
  }

  @Test
  public void selectExecutorsNoWorkerValid() throws Exception {
    mTestBlockInfo.setLocations(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)));
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        1,
        Lists.newArrayList(WORKER_INFO_1));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = ImmutableSet.of();
    // select none as no choice left
    assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsInsufficientWorkerValid() throws Exception {
    mTestBlockInfo.setLocations(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)));
    Set<Pair<WorkerInfo, SerializableVoid>> result = selectExecutorsTestHelper(
        2,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2));
    Set<Pair<WorkerInfo, SerializableVoid>> expected = Sets.newHashSet();
    expected.add(new Pair<>(WORKER_INFO_2, null));
    // select the only worker left though more copies are requested
    assertEquals(expected, result);
  }

  @Test
  public void runTaskNoBlockWorker() throws Exception {
    byte[] input = BufferUtils.getIncreasingByteArray(0, (int) TEST_BLOCK_SIZE);

    TestBlockInStream mockInStream =
        new TestBlockInStream(input, TEST_BLOCK_ID, input.length, false,
            BlockInStreamSource.NODE_LOCAL);
    TestBlockOutStream mockOutStream =
        new TestBlockOutStream(ByteBuffer.allocate(MAX_BYTES), TEST_BLOCK_SIZE);
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.NO_LOCAL_BLOCK_WORKER_REPLICATE_TASK
        .getMessage(TEST_BLOCK_ID));
    runTaskReplicateTestHelper(Lists.<BlockWorkerInfo>newArrayList(), mockInStream, mockOutStream);
  }

  @Test
  public void runTaskLocalBlockWorkerDifferentFileStatus() throws Exception {
    for (boolean persisted : new boolean[] {true, false}) {
      for (boolean pinned : new boolean[] {true, false}) {
        mTestStatus.getFileInfo().setPersisted(persisted)
            .setMediumTypes(pinned ? Sets.newHashSet(Constants.MEDIUM_MEM)
                : Collections.emptySet());
        byte[] input = BufferUtils.getIncreasingByteArray(0, (int) TEST_BLOCK_SIZE);
        TestBlockInStream mockInStream =
            new TestBlockInStream(input, TEST_BLOCK_ID, input.length, false,
                BlockInStreamSource.NODE_LOCAL);
        TestBlockOutStream mockOutStream =
            new TestBlockOutStream(ByteBuffer.allocate(MAX_BYTES), TEST_BLOCK_SIZE);
        BlockWorkerInfo localBlockWorker = new BlockWorkerInfo(LOCAL_ADDRESS, TEST_BLOCK_SIZE, 0);
        runTaskReplicateTestHelper(Lists.newArrayList(localBlockWorker), mockInStream,
            mockOutStream);
        assertEquals(TEST_BLOCK_SIZE, mockInStream.getBytesRead());
        if (!persisted || pinned) {
          assertArrayEquals(
              String.format("input-output mismatched: pinned=%s, persisted=%s", pinned, persisted),
              input, mockOutStream.getWrittenData());
        }
      }
    }
  }

  @Test
  public void runTaskInputIOException() throws Exception {
    // file is pinned on a medium
    mTestStatus.getFileInfo().setMediumTypes(Sets.newHashSet(Constants.MEDIUM_MEM));
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
