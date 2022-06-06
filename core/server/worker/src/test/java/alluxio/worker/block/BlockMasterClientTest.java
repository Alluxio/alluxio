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

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.grpc.BlockHeartbeatPRequest;
import alluxio.grpc.BlockHeartbeatPResponse;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.CommitBlockInUfsPRequest;
import alluxio.grpc.CommitBlockInUfsPResponse;
import alluxio.grpc.CommitBlockPRequest;
import alluxio.grpc.CommitBlockPResponse;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.grpc.GetRegisterLeasePResponse;
import alluxio.grpc.GetWorkerIdPRequest;
import alluxio.grpc.GetWorkerIdPResponse;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.Metric;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.ServiceType;
import alluxio.grpc.StorageList;
import alluxio.master.MasterClientContext;
import alluxio.retry.RetryUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.user.ServerUserState;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class BlockMasterClientTest {

  private static final InetSocketAddress TEST_SOCKET_ADDRESS =
      new InetSocketAddress("0.0.0.0", 9999);

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap
      .of(PropertyKey.MASTER_HOSTNAME, "localhost",
          PropertyKey.MASTER_RPC_PORT, 9999,
          PropertyKey.MASTER_RPC_ADDRESSES, ImmutableList.of(TEST_SOCKET_ADDRESS),
          PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s",
          PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS, "100ms",
          PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "500ms",
          PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL),
      Configuration.modifiableGlobal());

  // mock rpc server and channel managed by test suite
  private GrpcServer mServer;
  private GrpcChannel mChannel;

  private final AlluxioConfiguration mConf = Configuration.global();

  @Test
  public void testClientInfo() {
    BlockMasterClient client = new BlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create()).build());

    assertEquals(ServiceType.BLOCK_MASTER_WORKER_SERVICE, client.getRemoteServiceType());
    assertEquals(Constants.BLOCK_MASTER_WORKER_SERVICE_NAME, client.getServiceName());
    assertEquals(Constants.BLOCK_MASTER_WORKER_SERVICE_VERSION, client.getServiceVersion());
  }

  @Test
  public void convertBlockListMapToProtoMergeDirsInSameTier() {
    BlockMasterClient client = new BlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create()).build());

    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();
    BlockStoreLocation memDir0 = new BlockStoreLocation("MEM", 0);
    blockMap.put(memDir0, Arrays.asList(1L, 2L, 3L));
    BlockStoreLocation memDir1 = new BlockStoreLocation("MEM", 1);
    blockMap.put(memDir1, Arrays.asList(4L, 5L, 6L, 7L));
    BlockStoreLocation ssdDir0 = new BlockStoreLocation("SSD", 0);
    blockMap.put(ssdDir0, Arrays.asList(11L, 12L, 13L, 14L));
    BlockStoreLocation ssdDir1 = new BlockStoreLocation("SSD", 1);
    blockMap.put(ssdDir1, Arrays.asList(15L, 16L, 17L, 18L, 19L));

    // Directories on the same tier will be merged together
    List<LocationBlockIdListEntry> protoList = client.convertBlockListMapToProto(blockMap);
    assertEquals(2, protoList.size());
    BlockStoreLocationProto memLocationProto = BlockStoreLocationProto.newBuilder()
        .setTierAlias("MEM").setMediumType("").build();
    BlockStoreLocationProto ssdLocationProto = BlockStoreLocationProto.newBuilder()
        .setTierAlias("SSD").setMediumType("").build();
    Set<BlockStoreLocationProto> blockLocations = protoList.stream()
        .map(LocationBlockIdListEntry::getKey).collect(Collectors.toSet());
    assertEquals(ImmutableSet.of(memLocationProto, ssdLocationProto), blockLocations);

    LocationBlockIdListEntry firstEntry = protoList.get(0);
    if (firstEntry.getKey().getTierAlias().equals("MEM")) {
      LocationBlockIdListEntry memTierEntry = protoList.get(0);
      List<Long> memProtoBlockList = memTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
              new HashSet<>(memProtoBlockList));
      LocationBlockIdListEntry ssdTierEntry = protoList.get(1);
      List<Long> ssdProtoBlockList = ssdTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L),
              new HashSet<>(ssdProtoBlockList));
    } else {
      LocationBlockIdListEntry memTierEntry = protoList.get(1);
      List<Long> memProtoBlockList = memTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
              new HashSet<>(memProtoBlockList));
      LocationBlockIdListEntry ssdTierEntry = protoList.get(0);
      List<Long> ssdProtoBlockList = ssdTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L),
              new HashSet<>(ssdProtoBlockList));
    }
  }

  @Test
  public void testCommitBlock() throws Exception {
    ConcurrentHashMap<Long, Long> committedBlocks = new ConcurrentHashMap<>();
    final long workerId = 1L;
    final long blockId = 2L;
    final long usedBytesOnTier = 1024 * 1024L;
    final long length = 1024 * 1024L;
    final String tierAlias = "MEM";
    final String mediumType = "MEM";

    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void commitBlock(CommitBlockPRequest request,
                                  StreamObserver<CommitBlockPResponse> responseObserver) {
            long blockId = request.getBlockId();
            long workerId = request.getWorkerId();
            committedBlocks.put(blockId, workerId);
            responseObserver.onNext(CommitBlockPResponse.newBuilder().build());
            responseObserver.onCompleted();
          }
        });

    // create test client and redirect to our mock channel
    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mChannel
    );

    client.commitBlock(workerId, usedBytesOnTier, tierAlias, mediumType, blockId, length);

    assertEquals(1, committedBlocks.size());
    assertEquals(workerId, (long) committedBlocks.get(blockId));
  }

  @Test
  public void testCommitUfsBlock() throws Exception {
    ConcurrentHashMap<Long, Long> committedUfsBlocks = new ConcurrentHashMap<>();
    final long blockId = 1L;
    final long length = 1024 * 1024L;

    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void commitBlockInUfs(CommitBlockInUfsPRequest request,
                                       StreamObserver<CommitBlockInUfsPResponse> responseObserver) {
            long blockId = request.getBlockId();
            long length = request.getLength();
            committedUfsBlocks.put(blockId, length);
            responseObserver.onNext(CommitBlockInUfsPResponse.newBuilder().build());
            responseObserver.onCompleted();
          }
        });

    // create test client and redirect to our mock channel
    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mChannel
    );

    client.commitBlockInUfs(blockId, length);

    assertEquals(1, committedUfsBlocks.size());
    assertEquals(length, (long) committedUfsBlocks.get(blockId));
  }

  @Test
  public void testGetId() throws Exception {
    WorkerNetAddress testExistsAddress = new WorkerNetAddress();
    testExistsAddress.setTieredIdentity(new TieredIdentity(new ArrayList<>()));
    WorkerNetAddress testNonExistsAddress = new WorkerNetAddress();
    testNonExistsAddress.setHost("1.2.3.4");
    long workerId = 1L;
    Map<WorkerNetAddress, Long> workerIds = ImmutableMap.of(testExistsAddress, workerId);

    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void getWorkerId(GetWorkerIdPRequest request,
                                  StreamObserver<GetWorkerIdPResponse> responseObserver) {
            WorkerNetAddress address = GrpcUtils.fromProto(request.getWorkerNetAddress());
            Long id = workerIds.get(address);
            if (id == null) {
              id = -1L;
            }
            responseObserver.onNext(GetWorkerIdPResponse.newBuilder().setWorkerId(id).build());
            responseObserver.onCompleted();
          }
        });

    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mChannel);

    assertEquals(workerId, client.getId(testExistsAddress));
    assertEquals(-1L, client.getId(testNonExistsAddress));
  }

  @Test
  public void testHeartBeat() throws Exception {
    final long workerId = 1L;
    final Map<String, Long> capacityBytesOnTiers = ImmutableMap.of("MEM", 1024 * 1024L);
    final Map<String, Long> usedBytesOnTiers = ImmutableMap.of("MEM", 1024L);
    final List<Long> removedBlocks = ImmutableList.of();
    final Map<BlockStoreLocation, List<Long>> addedBlocks = ImmutableMap.of();
    final Map<String, List<String>> lostStorage = ImmutableMap.of(
        "MEM",
        ImmutableList.of("/tmp/lost")
    );
    final List<Metric> metrics = ImmutableList.of();

    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void blockHeartbeat(BlockHeartbeatPRequest request,
                                     StreamObserver<BlockHeartbeatPResponse> responseObserver) {
            responseObserver.onNext(
                BlockHeartbeatPResponse.newBuilder().setCommand(Command.newBuilder()
                .setCommandType(
                    CommandType.Nothing)).build()
            );
            responseObserver.onCompleted();
          }
        });

    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mChannel
    );

    assertEquals(CommandType.Nothing, client.heartbeat(
        workerId,
        capacityBytesOnTiers,
        usedBytesOnTiers,
        removedBlocks,
        addedBlocks,
        lostStorage,
        metrics).getCommandType());
  }

  @Test
  public void testAcquireRegisterLeaseFailure() {
    assertThrows(FailedToAcquireRegisterLeaseException.class,
        () -> testAcquireRegisterLease(false));
  }

  @Test
  public void testAcquireRegisterLeaseSuccess() throws Exception {
    testAcquireRegisterLease(true);
  }

  @Test
  public void testRegister() throws Exception {
    register(false);
  }

  @Test
  public void testRegisterStream() throws Exception {
    register(true);
  }

  private void testAcquireRegisterLease(boolean expectedSuccess) throws Exception {
    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void requestRegisterLease(
              GetRegisterLeasePRequest request,
              StreamObserver<GetRegisterLeasePResponse> responseObserver) {
            responseObserver.onNext(
                GetRegisterLeasePResponse.newBuilder().setAllowed(expectedSuccess).build());
            responseObserver.onCompleted();
          }
        });

    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mChannel
    );

    client.acquireRegisterLeaseWithBackoff(
        1L,
        1,
        RetryUtils.noRetryPolicy());
  }

  public void register(boolean stream) throws Exception {
    final long workerId = 1L;
    final List<String> storageTierAliases = ImmutableList.of("MEM");
    final Map<String, Long> totalBytesOnTiers = ImmutableMap.of("MEM", 1024 * 1024L);
    final Map<String, Long> usedBytesOnTiers = ImmutableMap.of("MEM", 1024L);
    // currently have one block at (MEM, 0, MEM)
    // dir-index is set to 0 for recovery from proto
    final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation = ImmutableMap.of(
        new BlockStoreLocation("MEM", 0, "MEM"), ImmutableList.of(0L)
    );
    final Map<String, List<String>> lostStorage = ImmutableMap.of("MEM", ImmutableList.of());
    final List<ConfigProperty> configList = ImmutableList.of();

    List<Long> registeredWorkerIds = new ArrayList<>();

    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void registerWorker(RegisterWorkerPRequest request,
                                     StreamObserver<RegisterWorkerPResponse> responseObserver) {

            // verify request data
            assertEquals(workerId, request.getWorkerId());
            assertEquals(storageTierAliases, request.getStorageTiersList());
            assertEquals(totalBytesOnTiers, request.getTotalBytesOnTiersMap());
            assertEquals(usedBytesOnTiers, request.getUsedBytesOnTiersMap());

            // verify that we get all the block information
            for (LocationBlockIdListEntry entry: request.getCurrentBlocksList()) {
              BlockStoreLocationProto locationProto = entry.getKey();
              BlockStoreLocation location = new BlockStoreLocation(
                  locationProto.getTierAlias(), 0, locationProto.getMediumType());
              List<Long> blockIdList = currentBlocksOnLocation.get(location);
              assert blockIdList != null
                  && blockIdList.containsAll(entry.getValue().getBlockIdList());
            }

            assertEquals(lostStorage.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                    e -> StorageList.newBuilder().addAllStorage(e.getValue()).build())),
                request.getLostStorageMap());

            registeredWorkerIds.add(request.getWorkerId());
            responseObserver.onNext(RegisterWorkerPResponse.newBuilder().build());
            responseObserver.onCompleted();
          }

          @Override
          public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
              StreamObserver<RegisterWorkerPResponse> responseObserver) {
            return new StreamObserver<RegisterWorkerPRequest>() {
              @Override
              public void onNext(RegisterWorkerPRequest value) {
                registeredWorkerIds.add(value.getWorkerId());
                responseObserver.onNext(RegisterWorkerPResponse.newBuilder().build());
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        });

    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mChannel
    );

    if (stream) {
      client.registerWithStream(
          workerId,
          storageTierAliases,
          totalBytesOnTiers,
          usedBytesOnTiers,
          currentBlocksOnLocation,
          lostStorage,
          configList
      );
    } else {
      client.register(
          workerId,
          storageTierAliases,
          totalBytesOnTiers,
          usedBytesOnTiers,
          currentBlocksOnLocation,
          lostStorage,
          configList);
    }

    assertEquals(1, registeredWorkerIds.size());
    assertEquals(workerId, (long) registeredWorkerIds.get(0));
  }

  @After
  public void cleanUp() {
    if (mServer != null && mServer.isServing()) {
      mServer.shutdown();
    }

    if (mChannel != null && !mChannel.isShutdown()) {
      mChannel.shutdown();
    }

    mChannel = null;
    mServer = null;
  }

  // create a mock grpc server that uses delegate to handle rpc calls
  private void createMockService(
      BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase delegate) throws IOException {

    // make sure to clean up resources
    cleanUp();

    // create mock service handler
    BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase mockService =
        mock(
            BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase.class,
            delegatesTo(delegate));

    // set up mock server with custom handler
    mServer = GrpcServerBuilder.forAddress(
        GrpcServerAddress.create("localhost", TEST_SOCKET_ADDRESS),
        mConf,
        ServerUserState.global()
    )
        .addService(ServiceType.BLOCK_MASTER_WORKER_SERVICE, new GrpcService(mockService))
        .build()
        .start();

    assert mServer.isServing();

    mChannel = GrpcChannelBuilder.newBuilder(
        GrpcServerAddress.create("localhost", TEST_SOCKET_ADDRESS),
        mConf).disableAuthentication().build();
  }

  // a sub-class of BlockMasterClient that re-direct grpc requests
  // to a mock channel provided by test functions
  private static class MockStubBlockMasterClient extends BlockMasterClient {
    public MockStubBlockMasterClient(
        MasterClientContext conf,
        GrpcChannel mockChannel) {
      super(conf);
      mChannel = mockChannel;
    }

    @Override
    public void connect() {
      // channel passed by test suite is always connected
      mConnected = true;
      afterConnect();
    }

    @Override
    public void disconnect() {
      // empty implementation
      // connection is cleaned up by test suite
    }
  }
}
