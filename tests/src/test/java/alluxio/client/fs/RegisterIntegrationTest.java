package alluxio.client.fs;

import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.clock.SystemClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetWorkerIdPResponse;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerStreamPRequest;
import alluxio.grpc.RegisterWorkerStreamPResponse;
import alluxio.grpc.StorageList;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterWorkerServiceHandler;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.meta.Block;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RegisterIntegrationTest {
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private Clock mClock;
  private ExecutorService mExecutorService;
  private MetricsMaster mMetricsMaster;
  private BlockMasterWorkerServiceHandler mHandler;


  ExecutorService mClientExecutorService;

  public static final Map<String, List<String>> LOST_STORAGE =
          ImmutableMap.of(MEM.toString(), ImmutableList.of());
  public static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();
  public static final long BLOCK_SIZE = ServerConfiguration.global().getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);

  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION =
          ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final int CONCURRENT_CLIENT_COUNT = 20;
  private static final long BLOCK1_ID = 1L;
  private static final long BLOCK1_LENGTH = 49L;
  private static final long BLOCK2_ID = 2L;
  private static final long BLOCK2_LENGTH = 59L;
  private static final Map<String, Long> MEM_CAPACITY = ImmutableMap.of("MEM", 100L);
  private static final Map<String, Long> MEM_USAGE_EMPTY = ImmutableMap.of("MEM", 0L);
  private static final Command FREE_BLOCK1_CMD = Command.newBuilder()
          .setCommandType(CommandType.Free).addData(1).build();
  private static final Command EMPTY_CMD = Command.newBuilder()
          .setCommandType(CommandType.Nothing).build();
  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set the config properties
    ServerConfiguration.set(PropertyKey.MASTER_REGISTER_WORKER_STREAM_TIMEOUT, "1s");

    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    // TODO(jiacheng): Use a manual clock in the test
//    mClock = new ManualClock();
    mClock = new SystemClock();

    mExecutorService =
            Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
    mHandler = new BlockMasterWorkerServiceHandler(mBlockMaster);

    mClientExecutorService = Executors.newFixedThreadPool(10,
            ThreadFactoryUtils.build("TestBlockMaster-%d", true));
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  public long getWorkerId(WorkerNetAddress address) throws Exception {
    StreamObserver<GetWorkerIdPResponse> noopResponseObserver =
            new StreamObserver<GetWorkerIdPResponse>() {
              @Override
              public void onNext(GetWorkerIdPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };
    long workerId = mBlockMaster.getWorkerId(address);
    System.out.println("Worker id " + workerId);
    return workerId;
  }

  public void prepareBlocksOnMaster(Map<BlockStoreLocation, List<Long>> blockMap) throws UnavailableException {
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      for (long blockId : entry.getValue()) {
        mBlockMaster.commitBlockInUFS(blockId, BLOCK_SIZE);
      }
    }
  }

  public void prepareBLocksOnMaster(List<Long> blockIds) throws UnavailableException {
    for (long id : blockIds) {
      mBlockMaster.commitBlockInUFS(id, BLOCK_SIZE);
    }
  }

  @Test
  public void registerWorkerStream() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  private static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  public Map<TierAlias, List<Integer>> convert(String tiersConfig) {
    String[] tiers = tiersConfig.split(";");
    int length = Math.min(tiers.length, TierAlias.values().length);
    ImmutableMap.Builder<TierAlias, List<Integer>> builder = new ImmutableMap.Builder<>();
    for (int i = 0; i < length; i++) {
      builder.put(
              TierAlias.SORTED.get(i),
              Arrays.stream(tiers[i].split(","))
                      .map(Integer::parseInt)
                      .collect(Collectors.toList()));
    }
    return builder.build();
  }

  @Test
  public void registerWorkerStreamAndDeleteBlock() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    RegisterWorkerStreamPRequest lastChunk = null;
    List<Long> removedBlocks = new ArrayList<>();
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      requestObserver.onNext(chunk);

      // Delete some blocks
      if (lastChunk != null) {
        long selectedBlock = selectABlock(lastChunk);
        System.out.println("Deleting block " + selectedBlock);
        BlockInfo blockInfo = mBlockMaster.getBlockInfo(selectedBlock);
        System.out.println("Block to delete: " + blockInfo);

        // TODO(jiacheng): before this worker finishes registration, removeBlocks will
        //  not touch the worker info so not acquiring locks on this worker at all.
        mBlockMaster.removeBlocks(ImmutableList.of(selectedBlock), false);
        removedBlocks.add(selectedBlock);
      }

      lastChunk = chunk;
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    System.out.println("Here are the removed blocks: " + removedBlocks);
    for (long rb : removedBlocks) {
      BlockInfo info = mBlockMaster.getBlockInfo(rb);
      System.out.println("Block " + rb + ": " + info);
    }

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  public long selectABlock(RegisterWorkerStreamPRequest request) {
    List<LocationBlockIdListEntry> entryList = request.getCurrentBlocksList();
    if (entryList.size() > 0) {
      LocationBlockIdListEntry entry = entryList.get(0);
      List<Long> blocks = entry.getValue().getBlockIdList();
      if (blocks.size() > 0) {
        return blocks.get(0);
      }
    }
    return -1;
  }

  // TODO(jiacheng): commit during stream register
  @Test
  public void registerWorkerStreamAndCommitBlock() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    RegisterWorkerStreamPRequest lastChunk = null;
    List<Long> committedBlocks = new ArrayList<>();
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      requestObserver.onNext(chunk);

      // Delete some blocks
      if (lastChunk != null) {
        long selectedBlock = selectABlock(lastChunk);
        System.out.println("Deleting block " + selectedBlock);
        BlockInfo blockInfo = mBlockMaster.getBlockInfo(selectedBlock);
        System.out.println("Block to delete: " + blockInfo);

        // TODO(jiacheng): Cannot commit a block on a worker that is not in mWorker!
        //  That means for a new worker that is impossible. What if it's a re-registering worker?
        // TODO(jiacheng): This used size is incorrect!
        mBlockMaster.commitBlock(workerId, BLOCK1_LENGTH, "MEM", "MEM", selectedBlock, BLOCK1_LENGTH);
        committedBlocks.add(selectedBlock);
      }

      lastChunk = chunk;
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    System.out.println("Here are the committed blocks: " + committedBlocks);
    for (long rb : committedBlocks) {
      BlockInfo info = mBlockMaster.getBlockInfo(rb);
      System.out.println("Block " + rb + ": " + info);
    }

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  @Test
  public void commitBlockOnReregisteringWorker() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);
    long workerId = getWorkerId(address);

    // Prepare an empty worker
    registerEmptyWorker(workerId, address);

    // Re-register with updated information
    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);
    // Some blocks that do not exist on the worker
    prepareBLocksOnMaster(ImmutableList.of(BLOCK1_ID, BLOCK2_ID));

    // Noop response observer
    // TODO(jiacheng): extract this
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    RegisterWorkerStreamPRequest lastChunk = null;
    Collection<Long> committedBlocks = new ConcurrentLinkedDeque<>();
    CountDownLatch latch = new CountDownLatch(requestChunks.size() - 1 + 2);
    int iter = 0;
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      System.out.println("Iter " + iter);
      // TODO(jiacheng): rate limit this? ACK until the next send?
      requestObserver.onNext(chunk);

      if (iter == 1) {
        for (long id : ImmutableList.of(BLOCK1_ID, BLOCK2_ID)) {
          Future future = mClientExecutorService.submit(() -> {
            try {
              mBlockMaster.commitBlock(workerId, BLOCK1_LENGTH, "MEM", "MEM", id, BLOCK1_LENGTH);
              committedBlocks.add(id);
              latch.countDown();
              System.out.println("Commit finished, now latch " + latch.getCount());
              return;
            } catch (NotFoundException e) {
              e.printStackTrace();
            } catch (UnavailableException e) {
              e.printStackTrace();
            }
          });
        }
      }

      // Commit some blocks
      if (lastChunk != null) {
        long selectedBlock = selectABlock(lastChunk);
        System.out.println("Commit block " + selectedBlock);
        BlockInfo blockInfo = mBlockMaster.getBlockInfo(selectedBlock);
        System.out.println("Block to commit: " + blockInfo);

        // TODO(jiacheng): This used size is incorrect!
        // Use another thread because the worker locks are re-entrant
        Future future = mClientExecutorService.submit(() -> {
          try {
            mBlockMaster.commitBlock(workerId, BLOCK1_LENGTH, "MEM", "MEM", selectedBlock, BLOCK1_LENGTH);
            committedBlocks.add(selectedBlock);
            latch.countDown();
            System.out.println("Commit finished, now latch " + latch.getCount());
            return;
          } catch (NotFoundException e) {
            e.printStackTrace();
          } catch (UnavailableException e) {
            e.printStackTrace();
          }
        });
      }

      lastChunk = chunk;
      iter++;
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    latch.await();
    System.out.println(requestChunks.size() + " blocks all committed. Here are the committed blocks: " + committedBlocks);
    for (long rb : committedBlocks) {
      BlockInfo info = mBlockMaster.getBlockInfo(rb);
      System.out.println("Block " + rb + ": " + info);
    }

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000+2, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  public void registerEmptyWorker(long workerId, WorkerNetAddress address) throws Exception {
    mBlockMaster.workerRegister(workerId, Arrays.asList("MEM"), MEM_CAPACITY,
            MEM_USAGE_EMPTY, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
  }

  // TODO(jiacheng): reregister, ConcurrentModEx?
  @Test
  public void reregister() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    RegisterWorkerStreamPRequest lastChunk = null;
    List<Long> committedBlocks = new ArrayList<>();
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      requestObserver.onNext(chunk);

      // Delete some blocks
      if (lastChunk != null) {
        long selectedBlock = selectABlock(lastChunk);
        System.out.println("Deleting block " + selectedBlock);
        BlockInfo blockInfo = mBlockMaster.getBlockInfo(selectedBlock);
        System.out.println("Block to delete: " + blockInfo);

        // TODO(jiacheng): Cannot commit a block on a worker that is not in mWorker!
        //  That means for a new worker that is impossible. What if it's a re-registering worker?
        // TODO(jiacheng): This used size is incorrect!
        mBlockMaster.commitBlock(workerId, BLOCK1_LENGTH, "MEM", "MEM", selectedBlock, BLOCK1_LENGTH);
        committedBlocks.add(selectedBlock);
      }

      lastChunk = chunk;
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    System.out.println("Here are the committed blocks: " + committedBlocks);
    for (long rb : committedBlocks) {
      BlockInfo info = mBlockMaster.getBlockInfo(rb);
      System.out.println("Block " + rb + ": " + info);
    }

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  @Test
  public void registerWorkerStreamBrokenClient() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    ErrorBlockMasterWorkerServiceHandler brokenHandler = new ErrorBlockMasterWorkerServiceHandler(mHandler);
    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            brokenHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    requestObserver.onNext(requestChunks.get(0));
    StatusException x = new InternalException(new RuntimeException("Error on the client side")).toGrpcStatusException();
    requestObserver.onError(x);

    // verify the worker is not
    assertEquals(1, brokenHandler.mErrors.size());
    assertEquals(1000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(0, mBlockMaster.getWorkerCount());

    // This should be empty, unregistered worker is in the mTempWorkers, not mWorkers or mLostWorkers
    List<WorkerInfo> workerInfos = mBlockMaster.getWorkerReport(GetWorkerReportOptions.defaults());
    System.out.println("Workers: " + workerInfos);

    // Re-register
    System.out.println("Retrying");
    requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  // TODO(jiacheng): Client hangs, the worker is unlocked
  @Test
  public void hangingworker() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                // TODO(jiacheng): If I receive an error, I should close on this side and stop sending more
                System.out.format("Error " + t);
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            mHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      //  The 2nd chunk will receive an error, then keep sending should get rejected
      requestObserver.onNext(chunk);



      // TODO(jiacheng): timeout not triggered!
      System.out.println("Sleep 5s on the client side, should trigger timeout");
      CommonUtils.sleepMs(5000);
    }
    // TODO(jiacheng): This should be rejected too
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }



  // Throws an exception on the server side on the 3rd request in the stream.
  class ErrorBlockMasterWorkerServiceHandler {
    private BlockMasterWorkerServiceHandler mDelegate;
    List<Throwable> mErrors = new ArrayList<>();

    public ErrorBlockMasterWorkerServiceHandler(BlockMasterWorkerServiceHandler delegate) {
      mDelegate = delegate;
    }

    public io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerStreamPRequest> registerWorkerStream(
            io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerStreamPResponse> responseObserver) {
      io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerStreamPRequest> requestObserver = mDelegate.registerWorkerStream(responseObserver);

      return new StreamObserver<alluxio.grpc.RegisterWorkerStreamPRequest>() {
        private int batch = 0;

        @Override
        public void onNext(alluxio.grpc.RegisterWorkerStreamPRequest chunk) {
          requestObserver.onNext(chunk);
          if (batch == 2) {
            // TODO(jiacheng): A better exception from master side?
            StatusException x = new InternalException(new RuntimeException("Error on the server side")).toGrpcStatusException();
            requestObserver.onError(x);
            batch++;
            return;
          }

          batch++;
        }

        @Override
        public void onError(Throwable t) {
          mErrors.add(t);
          requestObserver.onError(t);
        }

        @Override
        public void onCompleted() {
          requestObserver.onCompleted();
        }
      };
    }
  }

  @Test
  public void registerWorkerStreamBrokenServer() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Noop response observer
    StreamObserver<RegisterWorkerStreamPResponse> noopResponseObserver =
            new StreamObserver<RegisterWorkerStreamPResponse>() {
              @Override
              public void onNext(RegisterWorkerStreamPResponse response) {
                System.out.format("Response %s%n", response);
              }

              @Override
              public void onError(Throwable t) {
                // TODO(jiacheng): If I receive an error, I should close on this side and stop sending more
                System.out.format("Error " + t);

              }

              @Override
              public void onCompleted() {
                System.out.println("Completed");
              }
            };

    ErrorBlockMasterWorkerServiceHandler brokenHandler = new ErrorBlockMasterWorkerServiceHandler(mHandler);
    StreamObserver<RegisterWorkerStreamPRequest> requestObserver =
            brokenHandler.registerWorkerStream(noopResponseObserver);

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null, null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerStreamPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    for (RegisterWorkerStreamPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      //  The 2nd chunk will receive an error, then keep sending should get rejected
      requestObserver.onNext(chunk);
    }
    // TODO(jiacheng): This should be rejected too
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }


  // TODO(jiacheng): kill master in streaming, the worker should see error

  // TODO(jiacheng): fails in workerRegisterStart

  // TODO(jiacheng): fails in workerRegisterStream

  // TODO(jiacheng): fails in workerRegisterComplete

  // TODO(jiacheng): re-register unforgotten worker

  // TODO(jiacheng): re-register forgotten worker
}

