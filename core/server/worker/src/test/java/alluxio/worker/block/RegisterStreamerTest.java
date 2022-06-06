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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.Scope;
import alluxio.grpc.ServiceType;
import alluxio.security.authentication.AuthType;
import alluxio.security.user.ServerUserState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class RegisterStreamerTest {
  private static final InetSocketAddress TEST_ADDRESS = new InetSocketAddress("localhost", 9999);

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(
          new ImmutableMap.Builder<PropertyKey, Object>()
          // set response timeout short to ensure that tests don't take too long
          .put(PropertyKey.WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT, 100)
          .put(PropertyKey.WORKER_REGISTER_STREAM_DEADLINE, 1000)
          .put(PropertyKey.WORKER_REGISTER_STREAM_COMPLETE_TIMEOUT, 100)
          // set batch size to 1 so that the stream will send a request
          // for every block, used for easily control the concurrency of the stream
          .put(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, 1)
          // we don't serve authentication requests in testing
          .put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL)
          .build(),
          Configuration.modifiableGlobal());

  private GrpcServer mServer;
  private GrpcChannel mChannel;

  @Test(timeout = 2000)
  public void testRegisterTimeOutConcurrentRequest() throws Exception {
    // create a server that does not respond to requests
    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
              StreamObserver<RegisterWorkerPResponse> responseObserver) {
            return new StreamObserver<RegisterWorkerPRequest>() {
              @Override
              public void onNext(RegisterWorkerPRequest value) {
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
              }
            };
          }
        });

    // here we send 5 concurrent requests on the fly
    // the streamer should throw DeadlineExceededException
    // because it hasn't received enough response before sending further request
    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, 1L, 5);
    assertThrows(DeadlineExceededException.class, streamer::registerWithMaster);
  }

  @Test(timeout = 2000)
  public void testRegisterTimeOutSingleRequest() throws Exception {
    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
              StreamObserver<RegisterWorkerPResponse> responseObserver) {
            return new StreamObserver<RegisterWorkerPRequest>() {
              @Override
              public void onNext(RegisterWorkerPRequest value) {
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
              }
            };
          }
        });

    // the streamer should throw DeadlineExceededException
    // because the request gets no response
    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, 0L, 1);
    assertThrows(DeadlineExceededException.class, streamer::registerWithMaster);
  }

  @Test(timeout = 2000)
  public void testRegisterTimeOutNoCompletion() throws Exception {
    // create a mock server that responds to requests
    // properly but doesn't complete the stream
    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
              StreamObserver<RegisterWorkerPResponse> responseObserver) {
            return new StreamObserver<RegisterWorkerPRequest>() {
              @Override
              public void onNext(RegisterWorkerPRequest value) {
                responseObserver.onNext(RegisterWorkerPResponse.newBuilder().build());
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
              }
            };
          }
        });

    // stream should throw exception because server doesn't complete
    // the stream
    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, 0L, 5);
    assertThrows(DeadlineExceededException.class, streamer::registerWithMaster);
  }

  @Test(timeout = 2000)
  public void testServerEarlyError() throws Exception {
    // create a server that errors on the first request
    createMockService(new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
      @Override
      public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
          StreamObserver<RegisterWorkerPResponse> responseObserver) {
        // make sure we only error on the first request
        AtomicInteger counter = new AtomicInteger(0);
        return new StreamObserver<RegisterWorkerPRequest>() {
          @Override
          public void onNext(RegisterWorkerPRequest value) {
            if (counter.getAndIncrement() == 0) {
              responseObserver.onError(new RuntimeException());
            }
          }

          @Override
          public void onError(Throwable t) {
          }

          @Override
          public void onCompleted() {
          }
        };
      }
    });

    // create a streamer that sends 5 requests
    // it should catch the error and throw an InternalException
    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, 1L, 1);
    assertThrows(InternalException.class, streamer::registerWithMaster);
  }

  @Test(timeout = 2000)
  public void testServerEarlyComplete() throws Exception {
    // create a server that falsely completes on the first request
    createMockService(new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
      @Override
      public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
          StreamObserver<RegisterWorkerPResponse> responseObserver) {
        // make sure we only complete once
        AtomicInteger counter = new AtomicInteger(0);
        return new StreamObserver<RegisterWorkerPRequest>() {
          @Override
          public void onNext(RegisterWorkerPRequest value) {
            if (counter.getAndIncrement() == 0) {
              responseObserver.onCompleted();
            }
          }

          @Override
          public void onError(Throwable t) {
          }

          @Override
          public void onCompleted() {
          }
        };
      }
    });

    // create a streamer that sends 2 requests
    // it should notice that the stream is cancelled early on
    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, 1L, 1);
    assertThrows(CancelledException.class, streamer::registerWithMaster);
  }

  @Test(timeout = 2000)
  public void testServerErrorWhenComplete() throws Exception {
    // create a server that responds to requests normally but
    // throw an error when completing the stream
    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
              StreamObserver<RegisterWorkerPResponse> responseObserver) {
            return new StreamObserver<RegisterWorkerPRequest>() {
              @Override
              public void onNext(RegisterWorkerPRequest value) {
                responseObserver.onNext(RegisterWorkerPResponse.newBuilder().build());
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
                responseObserver.onError(new RuntimeException());
              }
            };
          }
        });

    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, 0L, 2);
    assertThrows(InternalException.class, streamer::registerWithMaster);
  }

  @Test(timeout = 6000)
  public void testCompleteStream() throws Exception {
    // store workerId to requestCount
    ConcurrentMap<Long, AtomicInteger> requestCount = new ConcurrentHashMap<>();
    // create a server that finally works
    createMockService(
        new BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase() {
          @Override
          public StreamObserver<RegisterWorkerPRequest> registerWorkerStream(
              StreamObserver<RegisterWorkerPResponse> responseObserver) {
            return new StreamObserver<RegisterWorkerPRequest>() {
              @Override
              public void onNext(RegisterWorkerPRequest value) {
                long workerId = value.getWorkerId();

                requestCount.putIfAbsent(workerId, new AtomicInteger(0));
                requestCount.get(workerId).incrementAndGet();

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

    // sends 5 requests for one workerId in total
    long workerId = 1L;
    RegisterStreamer streamer = createMockRegisterStreamer(mChannel, workerId, 5);
    streamer.registerWithMaster();
    assertEquals(1, requestCount.size());
    assertEquals(5, requestCount.get(workerId).get());
  }

  @After
  public void cleanUp() {
    if (mServer != null && mServer.isServing()) {
      mServer.shutdown();
    }

    if (mChannel != null && !mChannel.isShutdown()) {
      mChannel.shutdown();
    }

    mServer = null;
    mChannel = null;
  }

  // create a mock grpc server that delegates work to mockHandler.
  // this method modifies mServer and mChannel to the newly created server
  // and channel
  private void createMockService(
      BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase mockHandler) throws Exception {

    final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase mockService = mock(
        BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceImplBase.class,
        delegatesTo(mockHandler)
    );

    mServer = GrpcServerBuilder.forAddress(
        GrpcServerAddress.create(TEST_ADDRESS),
        Configuration.global(),
        ServerUserState.global()
    )
        .addService(ServiceType.BLOCK_MASTER_WORKER_SERVICE, new GrpcService(mockService))
        .build()
        .start();

    mChannel = GrpcChannelBuilder.newBuilder(
        GrpcServerAddress.create(TEST_ADDRESS),
        Configuration.global()
    )
        .build();
  }

  /**
   * Create a mocked RegisterStreamer Instance for test use.
   *
   * @param mockChannel mock channel for grpc redirection
   * @param workerId mock worker id for this streamer
   * @param numBlocks number of blocks in the stream, used to control concurrent requests
   * @return mocked RegisterStreamer instance
   */
  private RegisterStreamer createMockRegisterStreamer(
      Channel mockChannel, long workerId, long numBlocks) {
    final BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub mockClient =
        BlockMasterWorkerServiceGrpc.newStub(mockChannel);

    // mock some worker status parameters
    final List<String> storageTierAliases = ImmutableList.of("MEM", "SSD");
    final Map<String, Long> totalBytesOnTiers = ImmutableMap.of(
        "MEM",
        1024 * 1024 * 1024L,
        "SSD",
        1024 * 1024 * 1024 * 1024L
    );
    final Map<String, Long> usedBytesOnTiers = ImmutableMap.of(
        "MEM",
        1024 * 1024L,
        "SSD",
        1024 * 1024L
    );
    final Map<String, List<String>> lostStorage = ImmutableMap.of();
    final List<ConfigProperty> configList =
        Configuration.getConfiguration(Scope.WORKER);

    // current blocks in the worker, used for controlling how
    // many requests the stream tries to send
    List<Long> currentBlocks = LongStream.range(0L, numBlocks).boxed().collect(Collectors.toList());
    final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation = ImmutableMap.of(
        new BlockStoreLocation("MEM", 0, "MEM"),
        currentBlocks
    );

    return new RegisterStreamer(
        mockClient,
        workerId,
        storageTierAliases,
        totalBytesOnTiers,
        usedBytesOnTiers,
        currentBlocksOnLocation,
        lostStorage,
        configList
    );
  }
}
