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
import static org.mockito.Mockito.mock;
import static org.mockito.AdditionalAnswers.delegatesTo;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
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
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.Metric;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;
import alluxio.master.MasterClientContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.grpc.Channel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BlockMasterClientTest {

  @Rule
  public final GrpcCleanupRule mGrpcCleanup = new GrpcCleanupRule();

  private final AlluxioConfiguration mConf = Configuration.global();

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
    HashMap<Long, Long> committedBlocks = new HashMap<>();
    final long workerId = 0L;
    final long blockId = 0L;
    final long usedBytesOnTier = 1024 * 1024L;
    final long length = 1024 * 1024L;
    final String tierAlias = "MEM";
    final String mediumType = "MEM";

    Channel mockChannel = createMockHandler(
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
        mockChannel
    );

    assert client.mClient.getChannel() == mockChannel;
    client.commitBlock(workerId, usedBytesOnTier, tierAlias, mediumType, blockId, length);

    assertEquals(1, committedBlocks.size());
    assertEquals((Long) workerId, committedBlocks.get(blockId));
  }

  @Test
  public void testCommitUfsBlock() throws Exception {
    HashMap<Long, Long> committedUfsBlocks = new HashMap<>();
    final long blockId = 0L;
    final long length = 1024 * 1024L;

    Channel mockChannel = createMockHandler(
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
        mockChannel
    );

    client.commitBlockInUfs(blockId, length);

    assertEquals(1, committedUfsBlocks.size());
    assertEquals((Long) length, committedUfsBlocks.get(blockId));
  }

  @Test
  public void testGetId() throws Exception {
    WorkerNetAddress testExistsAddress = new WorkerNetAddress();
    testExistsAddress.setTieredIdentity(new TieredIdentity(new ArrayList<>()));
    WorkerNetAddress testNonExistsAddress = new WorkerNetAddress();
    testNonExistsAddress.setHost("1.2.3.4");
    Long workerId = 0L;
    Map<WorkerNetAddress, Long> workerIds = ImmutableMap.of(testExistsAddress, workerId);

    Channel mockChannel = createMockHandler(
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
        mockChannel);

    assertEquals((long) workerId, client.getId(testExistsAddress));
    assertEquals(-1L, client.getId(testNonExistsAddress));
  }

  @Test
  public void testHeartBeat() throws Exception {
    final long workerId = 0L;
    final Map<String, Long> capacityBytesOnTiers = ImmutableMap.of("MEM", 1024 * 1024L);
    final Map<String, Long> usedBytesOnTiers = ImmutableMap.of("MEM", 1024L);
    final List<Long> removedBlocks = ImmutableList.of();
    final Map<BlockStoreLocation, List<Long>> addedBlocks = ImmutableMap.of();
    final Map<String, List<String>> lostStorage = ImmutableMap.of(
        "MEM",
        ImmutableList.of("/tmp/lost")
    );
    final List<Metric> metrics = ImmutableList.of();

    Channel mockChannel = createMockHandler(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void blockHeartbeat(BlockHeartbeatPRequest request,
                                     StreamObserver<BlockHeartbeatPResponse> responseObserver) {
            responseObserver.onNext(
                BlockHeartbeatPResponse.newBuilder().setCommand(Command.newBuilder().setCommandType(
                    CommandType.Nothing)).build()
            );
            responseObserver.onCompleted();
          }
        });

    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mockChannel
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

  @Test(expected = FailedToAcquireRegisterLeaseException.class)
  public void testAcquireRegisterLeaseFailure() throws Exception {
    testAcquireRegisterLease(false);
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
    // our mock server will grant lease for block count <= 5 but will reject
    // lease for block count > 5
    Channel mockChannel = createMockHandler(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void requestRegisterLease(
              GetRegisterLeasePRequest request,
              StreamObserver<GetRegisterLeasePResponse> responseObserver) {
            boolean allowed = request.getBlockCount() <= 5;
            responseObserver.onNext(
                GetRegisterLeasePResponse.newBuilder().setAllowed(allowed).build());
            responseObserver.onCompleted();
          }
        });

    BlockMasterClient client = new MockStubBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build(),
        mockChannel
    );

    int blockCount = expectedSuccess ? 5 : 10;

    client.acquireRegisterLeaseWithBackoff(
        0L,
        blockCount,
        ExponentialTimeBoundedRetry
            .builder()
            .withMaxDuration(Duration.of(500, ChronoUnit.MILLIS))
            .withInitialSleep(Duration.of(0, ChronoUnit.MILLIS))
            .withMaxSleep(Duration.of(100, ChronoUnit.MILLIS))
            .withSkipInitialSleep()
            .build());
  }

  public void register(boolean stream) throws Exception {
    final long workerId = 0L;
    final List<String> storageTierAliases = ImmutableList.of("MEM");
    final Map<String, Long> totalBytesOnTiers = ImmutableMap.of("MEM", 1024 * 1024L);
    final Map<String, Long> usedBytesOnTiers = ImmutableMap.of("MEM", 1024L);
    final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation = ImmutableMap.of();
    final Map<String, List<String>> lostStorage = ImmutableMap.of("MEM", ImmutableList.of());
    final List<ConfigProperty> configList = ImmutableList.of();

    List<Long> registeredWorkerIds = new ArrayList<>();

    Channel mockChannel = createMockHandler(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public void registerWorker(RegisterWorkerPRequest request,
                                     StreamObserver<RegisterWorkerPResponse> responseObserver) {
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
        mockChannel
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

  // create a mock grpc server that uses delegate to handle rpc calls
  private Channel createMockHandler(
      BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase delegate) throws IOException {

    // create mock service handler
    BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase mockService =
        mock(
            BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase.class,
            delegatesTo(delegate));

    // create a mock server using mock service
    String serverName = InProcessServerBuilder.generateName();
    mGrpcCleanup.register(InProcessServerBuilder
        .forName(serverName).directExecutor().addService(mockService).build().start());

    // return the channel to the mock server
    return mGrpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  private static class MockStubBlockMasterClient extends BlockMasterClient {
    public MockStubBlockMasterClient(
        MasterClientContext conf,
        Channel mockChannel) {
      super(conf);
      mClient = BlockMasterWorkerServiceGrpc.newBlockingStub(mockChannel);
      mAsyncClient = BlockMasterWorkerServiceGrpc.newStub(mockChannel);
    }

    @Override
    public void connect() {
      // empty implementation
    }

    @Override
    public void disconnect() {
      // empty implementation
    }
  }
}
