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

package alluxio.master.file.loadmanager;

import static alluxio.master.file.loadmanager.LoadManager.Load;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioMockUtil;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.loadmanager.load.LoadInfo;
import alluxio.master.file.loadmanager.LoadManager.Scheduler;
import alluxio.master.file.loadmanager.LoadManager.BlockBuffer;
import alluxio.resource.CloseableResource;
import alluxio.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LoadManagerTest {
  private static final int WORKER_NUMBER = 5;
  private static final int QUEUE_EMPTY = 0;
  private final FileSystemMaster mFileSystemMaster = mock(FileSystemMaster.class);
  private final FileSystemContext mFileSystemContext = mock(FileSystemContext.class);
  private final LoadManager mLoadManager = new LoadManager(mFileSystemMaster, mFileSystemContext);
  private final Scheduler mScheduler = spy(new Scheduler(mFileSystemMaster, mFileSystemContext));
  private final AtomicLong mLoadId = new AtomicLong();
  private final List<WorkerNetAddress> mAdd = Lists.newArrayList();
  private final WorkerNetAddress mSingleWorker = new WorkerNetAddress();
  private BlockWorkerClient mClient;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mScheduler.start();

    for (int i = 0; i < WORKER_NUMBER; i++) {
      WorkerNetAddress address = new WorkerNetAddress();
      mAdd.add(address);
    }

    mClient = mock(BlockWorkerClient.class);
    CloseableResource<BlockWorkerClient> closeableResource = mock(CloseableResource.class);
    when(closeableResource.get()).thenReturn(mClient);
    when(mFileSystemContext.acquireBlockWorkerClient(any(WorkerNetAddress.class)))
            .thenReturn(closeableResource);
  }

  @Test
  public void testSchedule() throws Exception {
    List<Load> loads = generateRandomLoads(5);
    int fileInfoCountPerLoad = 3;
    Mockito.doReturn(mAdd).when(mScheduler).getWorkerAddresses();

    for (Load load : loads) {
      List<FileInfo> fileInfos = generateRandomFileInfo(fileInfoCountPerLoad);
      Mockito.doReturn(fileInfos).when(mScheduler).listFileInfos(load);
      BlockBuffer blockBuffer = mock(BlockBuffer.class);
      try (MockedStatic<BlockBuffer> mockedStatic =
                   mockStatic(BlockBuffer.class)) {
        mockedStatic.when(() ->
                BlockBuffer.create(fileInfos)).thenReturn(blockBuffer);
        Mockito.doNothing().when(mScheduler).loadBlockBatch(
                any(BlockWorkerClient.class), any(WorkerNetAddress.class), any(BlockBuffer.class), any(Load.class));
      }
    }

    for (Load load:loads) {
      mScheduler.schedule(load);
    }

    Thread.sleep(1000);
    AtomicInteger size = AlluxioMockUtil.getInternalState(mScheduler, "mCurrentSize");
    Assert.assertEquals(size.get(), QUEUE_EMPTY);
  }

  @Test
  public void testLoadBlockBatchSuccess() {
    int batchSize = 5;
    int iteration = 6;
    BlockBuffer blockBuffer = mock(BlockBuffer.class);
    LoadInfo info = generateRandomLoadInfo();
    Load load = createLoad(info);
    List<List<Block>> allBufferedLists = Lists.newArrayList();
    List<LoadRequest> requests = Lists.newArrayList();

    for (int i = 0; i < iteration; i++) {
      boolean isEnd;
      isEnd = i == iteration - 1;
      List<Block> blockBatch = generateRandomBlockList(batchSize, isEnd);
      allBufferedLists.add(blockBatch);
      LoadRequest loadRequest = Scheduler.buildRequest(load, blockBatch);
      requests.add(loadRequest);
      LoadResponse loadResponse = LoadResponse.newBuilder()
              .setStatus(TaskStatus.SUCCESS)
              .build();
      ListenableFuture<LoadResponse> listenableFuture = Futures.immediateFuture(loadResponse);
      when(mClient.load(loadRequest)).thenReturn(listenableFuture);
    }

    Iterator<List<Block>> iterator = allBufferedLists.iterator();
    when(blockBuffer.hasNext()).thenAnswer(inv -> iterator.hasNext());
    when(blockBuffer.getNextFileBlockBatch()).thenAnswer(inv -> iterator.next());

    mScheduler.loadBlockBatch(mClient, mSingleWorker, blockBuffer, load);

    for (LoadRequest request: requests) {
      verify(mClient).load(request); // verify client.load is called multiple times recursively
    }
    Assert.assertFalse(blockBuffer.hasNext()); // verify buffer iterates to the end
  }

  @Test
  public void testLoadBlockBatchPartialFailure() {
    int blockStatusOK = 8, blockStatusRetry = 4, blockStatusFail = 6;
    int numBatches = 3;
    FileInfo fileInfo = createFileInfo(Optional.of(BlockBuffer.BATCH_SIZE * numBatches));
    List<Block> allBlocks = fileInfo.getBlockIds().stream()
            .map(id -> BlockBuffer.buildBlock(fileInfo, id))
            .collect(Collectors.toList());
    List<List<Block>> initialPartitions = Lists.partition(allBlocks, BlockBuffer.BATCH_SIZE);

    BlockBuffer blockBuffer = BlockBuffer.create(Collections.singletonList(fileInfo));
    LoadInfo info = generateRandomLoadInfo();
    Load load = createLoad(info);
    List<LoadRequest> requests = Lists.newArrayList();
    List<BlockStatus> allRetriedBlockStatus = Lists.newArrayList();

    for (List<Block> blockBatch: initialPartitions) {
      LoadRequest loadRequest = Scheduler.buildRequest(load, blockBatch);
      requests.add(loadRequest);

      List<BlockStatus> blockStatusListOK = generateRandomBlockStatus(
              blockStatusOK, true, Status.Code.OK.value());
      List<BlockStatus> blockStatusListRetry = generateRandomBlockStatus(
              blockStatusRetry, true, Status.Code.UNKNOWN.value());
      List<BlockStatus> blockStatusListFail = generateRandomBlockStatus(
              blockStatusFail, false, Status.Code.UNKNOWN.value());
      List<BlockStatus> allResponseBlocks = Streams.concat(blockStatusListOK.stream(),
                      blockStatusListRetry.stream(), blockStatusListFail.stream())
              .collect(Collectors.toList());
      allRetriedBlockStatus.addAll(blockStatusListRetry);

      LoadResponse loadResponse = LoadResponse.newBuilder()
              .setStatus(TaskStatus.PARTIAL_FAILURE)
              .addAllBlockStatus(allResponseBlocks)
              .build();
      ListenableFuture<LoadResponse> listenableFuture = Futures.immediateFuture(loadResponse);
      when(mClient.load(loadRequest)).thenReturn(listenableFuture);
    }

    List<Block> allRetriedBlocks = allRetriedBlockStatus.stream()
            .map(blockStatus -> blockStatus.getBlock())
            .collect(Collectors.toList());
    //The retried blocks are called in integer segment of BlockBuffer.BATCH_SIZE in this test
    List<List<Block>> partitions = Lists.partition(allRetriedBlocks, BlockBuffer.BATCH_SIZE);

    for (List<Block> blockBatch: partitions) {
      LoadRequest loadRequest = Scheduler.buildRequest(load, blockBatch);
      requests.add(loadRequest);
      LoadResponse loadResponse = LoadResponse.newBuilder()
              .setStatus(TaskStatus.PARTIAL_FAILURE)
              .build();
      ListenableFuture<LoadResponse> listenableFuture = Futures.immediateFuture(loadResponse);
      when(mClient.load(loadRequest)).thenReturn(listenableFuture);
    }

    mScheduler.loadBlockBatch(mClient, mSingleWorker, blockBuffer, load);

    // verify client.load is called multiple times recursively for both initial and retried calls
    for (LoadRequest request: requests) {
      verify(mClient).load(request);
    }

    Assert.assertFalse(blockBuffer.hasNext()); // verify buffer iterates to the end
  }

  @Test
  public void testBlockBufferForSingleFile() {
    int testFileBlockLengthRange = 500;
    for (int i = 0; i < testFileBlockLengthRange; i++) {
      FileInfo fileInfo = createFileInfo(Optional.empty());
      BlockBuffer blockBuffer = BlockBuffer.create(Collections.singletonList(fileInfo));
      List<Block> allBlocks = fileInfo.getBlockIds().stream()
              .map(id -> BlockBuffer.buildBlock(fileInfo, id))
              .collect(Collectors.toList());
      List<List<Block>> expected = Lists.partition(allBlocks, BlockBuffer.BATCH_SIZE);

      for (List<Block> blocks : expected) {
        boolean bufferHasNext = blockBuffer.hasNext();
        Assert.assertTrue(bufferHasNext);
        List<Block> oneList = blockBuffer.getNextFileBlockBatch();
        Assert.assertEquals(oneList, blocks);
      }
      Assert.assertFalse(blockBuffer.hasNext());
      Assert.assertTrue(blockBuffer.getNextFileBlockBatch().isEmpty());
    }
  }

  @Test
  public void testBlockBufferForMultipleFiles() {
    int testFileCountRange = 200;
    for (int file = 0; file < testFileCountRange; file++) {
      List<FileInfo> fileInfos = generateRandomFileInfo(file);
      BlockBuffer blockBuffer = BlockBuffer.create(fileInfos);
      List<Block> allBlocks = fileInfos.stream().flatMap(fileInfo -> fileInfo.getBlockIds().stream()
              .map(id -> BlockBuffer.buildBlock(fileInfo, id))).collect(Collectors.toList());
      List<List<Block>> expected = Lists.partition(allBlocks, BlockBuffer.BATCH_SIZE);

      for (List<Block> block: expected) {
        boolean bufferHasNext = blockBuffer.hasNext();
        Assert.assertTrue(bufferHasNext);
        List<Block> oneList = blockBuffer.getNextFileBlockBatch();
        Assert.assertEquals(oneList, block);
      }
      Assert.assertFalse(blockBuffer.hasNext());
      Assert.assertTrue(blockBuffer.getNextFileBlockBatch().isEmpty());
    }
  }

  @Test
  public void testBlockBufferForMultipleFilesWithRetry() {
    int fileCount = 50;
    int retryListLength = 200;
    for (int file = 0; file < fileCount; file++) {
      for (int len = 0; len < retryListLength; len++) {
        List<FileInfo> fileInfos = generateRandomFileInfo(file);
        BlockBuffer blockBuffer = BlockBuffer.create(fileInfos);

        List<Block> retriedBlocks = generateRandomBlockList(len, false);
        retriedBlocks.forEach(blockBuffer::addToRetry);

        Stream<Block> allFileBlocks = fileInfos.stream().flatMap(fileInfo -> fileInfo.getBlockIds().stream()
                .map(id -> BlockBuffer.buildBlock(fileInfo, id)));
        List<Block> allBlocks = Streams.concat(allFileBlocks, retriedBlocks.stream()).collect(Collectors.toList());
        List<List<Block>> expected = Lists.partition(allBlocks, BlockBuffer.BATCH_SIZE);

        for (List<Block> block: expected) {
          boolean bufferHasNext = blockBuffer.hasNext();
          Assert.assertTrue(bufferHasNext);
          List<Block> oneList = blockBuffer.getNextFileBlockBatch();
          Assert.assertEquals(oneList, block);
        }
        Assert.assertFalse(blockBuffer.hasNext());
        Assert.assertTrue(blockBuffer.getNextFileBlockBatch().isEmpty());
      }
    }
  }

  private List<BlockStatus> generateRandomBlockStatus(int count, boolean retry, int code) {
    List<BlockStatus> blockStatusList = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      Block block = generateRandomBlockList(1, false).get(0);
      BlockStatus blockStatus = BlockStatus.newBuilder()
              .setRetryable(retry)
              .setBlock(block)
              .setCode(code)
              .build();
      blockStatusList.add(blockStatus);
    }
    return blockStatusList;
  }

  private List<Load> generateRandomLoads(int count) {
    List<Load> loads = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      LoadInfo info = generateRandomLoadInfo();
      loads.add(createLoad(info));
    }
    return loads;
  }

  private List<FileInfo> generateRandomFileInfo(int count) {
    List<FileInfo> fileInfos = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      FileInfo info = createFileInfo(Optional.empty());
      fileInfos.add(info);
    }
    return fileInfos;
  }

  private FileInfo createFileInfo(Optional<Integer> blockListLength) {
    int max = 100;
    int min = 0;
    Random random = new Random();
    FileInfo info = new FileInfo();
    String ufs = CommonUtils.randomAlphaNumString(6);
    long blockSize = Math.abs(random.nextInt());
    info.setUfsPath(ufs);
    info.setBlockSizeBytes(blockSize);
    int length = random.nextInt(max - min) + min; // use random length by default
    if (blockListLength.isPresent()) {
      length = blockListLength.get();
    }
    List<Long> blockIds = createBlockIdList(length);
    info.setBlockIds(blockIds);
    info.setFileBlockInfos(createFileBlockInfoList(blockIds));
    return info;
  }

  private List<Long> createBlockIdList(int length) {
    List<Long> blockIds = Lists.newArrayList();
    for (int i = 0; i < length; i ++) {
      long blockId = new Random().nextInt(1000);
      blockIds.add(blockId);
    }
    return blockIds;
  }

  private List<FileBlockInfo> createFileBlockInfoList(List<Long> blockIds) {
    List<FileBlockInfo> fileBlockInfos = Lists.newArrayList();
    for (long id: blockIds) {
      FileBlockInfo fileBlockInfo = createFileBlockInfo(id);
      fileBlockInfos.add(fileBlockInfo);
    }
    return fileBlockInfos;
  }

  private FileBlockInfo createFileBlockInfo(long id) {
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    BlockInfo blockInfo = new BlockInfo();
    blockInfo.setBlockId(id);
    fileBlockInfo.setBlockInfo(blockInfo);
    fileBlockInfo.setOffset(new Random().nextInt(1000));
    return fileBlockInfo;
  }

  private List<Block> generateRandomBlockList(int listLength, boolean isEnd) {
    List<Block> blocks = Lists.newArrayList();
    if (isEnd) {
      return blocks;
    }

    for (int i = 0; i < listLength; i++) {
      long blockSize = Math.abs(new Random().nextInt());
      long blockId = new Random().nextInt(1000);
      long mountId = new Random().nextInt(1000);
      long offset = new Random().nextInt(200);
      String ufs = CommonUtils.randomAlphaNumString(6);
      Block block = Block.newBuilder().setBlockId(blockId)
              .setBlockSize(blockSize)
              .setUfsPath(ufs)
              .setMountId(mountId)
              .setOffsetInFile(offset)
              .build();
      blocks.add(block);
    }
    return blocks;
  }

  private LoadInfo generateRandomLoadInfo() {
    return new LoadInfo(mLoadId.incrementAndGet(),
            CommonUtils.randomAlphaNumString(5), 1);
  }

  private Load createLoad(LoadInfo loadInfo) {
    return new Load(loadInfo.getId(),
            loadInfo.getPath(), loadInfo.getLoadOptions());
  }
}
