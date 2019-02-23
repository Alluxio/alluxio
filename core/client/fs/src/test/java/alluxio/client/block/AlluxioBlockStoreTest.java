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

package alluxio.client.block;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.client.WriteType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.network.TieredIdentityFactory;
import alluxio.resource.DummyCloseableResource;
import alluxio.util.FileSystemOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tests for {@link AlluxioBlockStore}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public final class AlluxioBlockStoreTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  private static final long BLOCK_ID = 3L;
  private static final long BLOCK_LENGTH = 100L;
  private static final String WORKER_HOSTNAME_LOCAL =
      NetworkAddressUtils.getLocalHostName((int) sConf
          .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  private static final String WORKER_HOSTNAME_REMOTE = "remote";
  private static final WorkerNetAddress WORKER_NET_ADDRESS_LOCAL = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_LOCAL);
  private static final WorkerNetAddress WORKER_NET_ADDRESS_REMOTE = new WorkerNetAddress()
      .setHost(WORKER_HOSTNAME_REMOTE);
  private ClientCallStreamObserver mStreamObserver;
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

  /**
   * A mock class used to return controlled result when selecting workers.
   */
  @ThreadSafe
  private static class MockFileWriteLocationPolicy
      implements FileWriteLocationPolicy, BlockLocationPolicy {
    private List<WorkerNetAddress> mWorkerNetAddresses;
    private int mIndex;

    /**
     * Cosntructs this mock location policy with empty host list.
     */
    public MockFileWriteLocationPolicy(AlluxioConfiguration alluxioConf) {
      mIndex = 0;
      mWorkerNetAddresses =  Collections.emptyList();
    }

    /**
     * Constructs this mock policy that returns the given result, once a time, in the input order.
     *
     * @param addresses list of addresses this mock policy will return
     */
    public MockFileWriteLocationPolicy(List<WorkerNetAddress> addresses) {
      mWorkerNetAddresses = Lists.newArrayList(addresses);
      mIndex = 0;
    }

    public void setHosts(List<WorkerNetAddress> addresses) {
      mWorkerNetAddresses = Lists.newArrayList(addresses);
      mIndex = 0;
    }

    @Override
    public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
        long blockSizeBytes) {
      if (mWorkerNetAddresses.isEmpty()) {
        return null;
      }
      return mWorkerNetAddresses.get(mIndex++);
    }

    @Override
    public WorkerNetAddress getWorker(GetWorkerOptions options) {
      return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
    }
  }

  @Rule
  public ExpectedException mException = ExpectedException.none();

  private BlockMasterClient mMasterClient;
  private BlockWorkerClient mWorkerClient;
  private AlluxioBlockStore mBlockStore;
  private WorkerNetAddress mLocalAddr;
  private FileSystemContext mContext;

  @Before
  public void before() throws Exception {
    mMasterClient = PowerMockito.mock(BlockMasterClient.class);
    mWorkerClient = PowerMockito.mock(BlockWorkerClient.class);

    mContext = PowerMockito.mock(FileSystemContext.class);
    when(mContext.acquireBlockMasterClientResource())
        .thenReturn(new DummyCloseableResource<>(mMasterClient));
    when(mContext.getClientContext()).thenReturn(ClientContext.create(sConf));
    when(mContext.getConf()).thenReturn(sConf);
    mLocalAddr =
        new WorkerNetAddress().setHost(NetworkAddressUtils.getLocalHostName(
            (int) sConf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));

    mBlockStore = new AlluxioBlockStore(mContext,
        TieredIdentityFactory.fromString("node=" + WORKER_HOSTNAME_LOCAL, sConf));

    when(mContext.acquireBlockWorkerClient(any(WorkerNetAddress.class)))
        .thenReturn(mWorkerClient);
    mStreamObserver = PowerMockito.mock(ClientCallStreamObserver.class);
    when(mWorkerClient.writeBlock(any(StreamObserver.class)))
        .thenReturn(mStreamObserver);
    when(mWorkerClient.openLocalBlock(any(StreamObserver.class)))
        .thenReturn(mStreamObserver);
    when(mStreamObserver.isReady()).thenReturn(true);
  }

  @Test
  public void getOutStreamUsingLocationPolicy() throws Exception {
    OutStreamOptions options = OutStreamOptions.defaults(sConf).setWriteType(WriteType.MUST_CACHE)
        .setLocationPolicy((workerInfoList, blockSizeBytes) -> {
          throw new RuntimeException("policy threw exception");
        });
    mException.expect(Exception.class);
    mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
  }

  @Test
  public void getOutStreamMissingLocationPolicy() throws IOException {
    OutStreamOptions options =
        OutStreamOptions.defaults(sConf).setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE).setLocationPolicy(null);
    mException.expect(NullPointerException.class);
    mException.expectMessage(PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED.toString());
    mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
  }

  @Test
  public void getOutStreamNoWorker() throws IOException {
    OutStreamOptions options =
        OutStreamOptions
            .defaults(sConf)
            .setBlockSizeBytes(BLOCK_LENGTH)
            .setWriteType(WriteType.MUST_CACHE)
            .setLocationPolicy(
                new MockFileWriteLocationPolicy(Lists.<WorkerNetAddress>newArrayList()));
    mException.expect(UnavailableException.class);
    mException
        .expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(BLOCK_LENGTH));
    mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
  }

  @Test
  public void getOutStreamLocal() throws Exception {
    File file = File.createTempFile("test", ".tmp");
    CreateLocalBlockResponse response = CreateLocalBlockResponse.newBuilder()
        .setPath(file.getAbsolutePath()).build();
    when(mWorkerClient.createLocalBlock(any(StreamObserver.class)))
        .thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            StreamObserver<CreateLocalBlockResponse> observer =
                invocation.getArgumentAt(0, StreamObserver.class);
            observer.onNext(response);
            return mStreamObserver;
          }
        });

    OutStreamOptions options = OutStreamOptions.defaults(sConf).setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_LOCAL)))
        .setWriteType(WriteType.MUST_CACHE);
    BlockOutStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(WORKER_NET_ADDRESS_LOCAL, stream.getAddress());
  }

  @Test
  public void getOutStreamRemote() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1");
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2");
    OutStreamOptions options = OutStreamOptions.defaults(sConf).setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(Arrays.asList(worker1, worker2)))
        .setWriteType(WriteType.MUST_CACHE);
    BlockOutStream stream1 = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(worker1, stream1.getAddress());
    BlockOutStream stream2 = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);
    assertEquals(worker2, stream2.getAddress());
  }

  @Test
  public void getOutStreamWithReplicated() throws Exception {
    File file = File.createTempFile("test", ".tmp");
    CreateLocalBlockResponse response = CreateLocalBlockResponse.newBuilder()
        .setPath(file.getAbsolutePath()).build();
    when(mWorkerClient.createLocalBlock(any(StreamObserver.class)))
        .thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            StreamObserver<CreateLocalBlockResponse> observer =
                invocation.getArgumentAt(0, StreamObserver.class);
            observer.onNext(response);
            return mStreamObserver;
          }
        });

    when(mMasterClient.getWorkerInfoList()).thenReturn(Lists
        .newArrayList(new alluxio.wire.WorkerInfo().setAddress(WORKER_NET_ADDRESS_LOCAL),
            new alluxio.wire.WorkerInfo().setAddress(WORKER_NET_ADDRESS_REMOTE)));
    OutStreamOptions options = OutStreamOptions.defaults(sConf).setBlockSizeBytes(BLOCK_LENGTH)
        .setLocationPolicy(new MockFileWriteLocationPolicy(
            Lists.newArrayList(WORKER_NET_ADDRESS_LOCAL, WORKER_NET_ADDRESS_REMOTE)))
        .setWriteType(WriteType.MUST_CACHE).setReplicationMin(2);
    BlockOutStream stream = mBlockStore.getOutStream(BLOCK_ID, BLOCK_LENGTH, options);

    assertEquals(alluxio.client.block.stream.BlockOutStream.class, stream.getClass());
  }

  @Test
  public void getInStreamUfs() throws Exception {
    WorkerNetAddress worker1 = new WorkerNetAddress().setHost("worker1");
    WorkerNetAddress worker2 = new WorkerNetAddress().setHost("worker2");
    BlockInfo info = new BlockInfo().setBlockId(0);
    URIStatus dummyStatus =
        new URIStatus(new FileInfo().setPersisted(true).setBlockIds(Collections.singletonList(0L))
            .setFileBlockInfos(Collections.singletonList(new FileBlockInfo().setBlockInfo(info))));
    OpenFilePOptions readOptions = OpenFilePOptions.newBuilder()
        .setFileReadLocationPolicy(MockFileWriteLocationPolicy.class.getTypeName()).build();
    InStreamOptions options = new InStreamOptions(dummyStatus, readOptions, sConf);
    ((MockFileWriteLocationPolicy) options.getUfsReadLocationPolicy())
        .setHosts(Arrays.asList(worker1, worker2));
    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo());
    when(mMasterClient.getWorkerInfoList()).thenReturn(
        Arrays.asList(new WorkerInfo().setAddress(worker1), new WorkerInfo().setAddress(worker2)));

    // Location policy chooses worker1 first.
    assertEquals(worker1, mBlockStore.getInStream(BLOCK_ID, options).getAddress());
    // Location policy chooses worker2 second.
    assertEquals(worker2, mBlockStore.getInStream(BLOCK_ID, options).getAddress());
  }

  @Test
  public void getInStreamNoWorkers() throws Exception {
    URIStatus dummyStatus =
        new URIStatus(new FileInfo().setPersisted(true).setBlockIds(Collections.singletonList(0L)));
    InStreamOptions options =
        new InStreamOptions(dummyStatus, FileSystemOptions.openFileDefaults(sConf),
            sConf);
    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo());
    when(mMasterClient.getWorkerInfoList()).thenReturn(Collections.emptyList());

    mException.expect(UnavailableException.class);
    mException.expectMessage("No Alluxio worker available");
    mBlockStore.getInStream(BLOCK_ID, options).getAddress();
  }

  @Test
  public void getInStreamMissingBlock() throws Exception {
    URIStatus dummyStatus = new URIStatus(
        new FileInfo().setPersisted(false).setBlockIds(Collections.singletonList(0L)));
    InStreamOptions options =
        new InStreamOptions(dummyStatus, FileSystemOptions.openFileDefaults(sConf), sConf);
    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(new BlockInfo());
    when(mMasterClient.getWorkerInfoList()).thenReturn(Collections.emptyList());

    mException.expect(NotFoundException.class);
    mException.expectMessage("unavailable in both Alluxio and UFS");
    mBlockStore.getInStream(BLOCK_ID, options).getAddress();
  }

  @Test
  public void getInStreamLocal() throws Exception {
    WorkerNetAddress remote = new WorkerNetAddress().setHost("remote");
    WorkerNetAddress local = new WorkerNetAddress().setHost(WORKER_HOSTNAME_LOCAL);

    // Mock away gRPC usage.
    OpenLocalBlockResponse response = OpenLocalBlockResponse.newBuilder().setPath("/tmp").build();
    when(mWorkerClient.openLocalBlock(any(StreamObserver.class)))
            .thenAnswer(new Answer() {
              public Object answer(InvocationOnMock invocation) {
                StreamObserver<OpenLocalBlockResponse> observer =
                        invocation.getArgumentAt(0, StreamObserver.class);
                observer.onNext(response);
                observer.onCompleted();
                return mStreamObserver;
              }
            });

    BlockInfo info = new BlockInfo().setBlockId(BLOCK_ID).setLocations(Arrays
        .asList(new BlockLocation().setWorkerAddress(remote),
            new BlockLocation().setWorkerAddress(local)));

    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(info);
    assertEquals(local, mBlockStore.getInStream(BLOCK_ID, new InStreamOptions(
        new URIStatus(new FileInfo().setBlockIds(Lists.newArrayList(BLOCK_ID))), sConf))
                            .getAddress());
  }

  @Test
  public void getInStreamRemote() throws Exception {
    WorkerNetAddress remote1 = new WorkerNetAddress().setHost("remote1");
    WorkerNetAddress remote2 = new WorkerNetAddress().setHost("remote2");

    BlockInfo info = new BlockInfo().setBlockId(BLOCK_ID).setLocations(Arrays
        .asList(new BlockLocation().setWorkerAddress(remote1),
            new BlockLocation().setWorkerAddress(remote2)));

    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(info);
    // We should sometimes get remote1 and sometimes get remote2.
    Set<WorkerNetAddress> results = new HashSet<>();
    for (int i = 0; i < 40; i++) {
      results.add(mBlockStore.getInStream(BLOCK_ID, new InStreamOptions(
          new URIStatus(new FileInfo().setBlockIds(Lists.newArrayList(BLOCK_ID))), sConf))
          .getAddress());
    }
    assertEquals(Sets.newHashSet(remote1, remote2), results);
  }

  @Test
  public void getInStreamInAlluxioOnlyFallbackToAvailableWorker() throws Exception {
    int workerCount = 4;
    boolean persisted = false;
    int[] blockLocations = new int[]{2, 3};
    Map<Integer, Long> failedWorkers = ImmutableMap.of(
        0, 3L,
        1, 1L,
        3, 2L);
    int expectedWorker = 2;
    testGetInStreamFallback(workerCount, persisted, blockLocations, failedWorkers, expectedWorker);
  }

  @Test
  public void getInStreamPersistedAndInAlluxioFallbackToUFS() throws Exception {
    int workerCount = 3;
    boolean persisted = true;
    int[] blockLocations = new int[]{0, 2};
    Map<Integer, Long> failedWorkers = ImmutableMap.of(
        0, 5L,
        2, 2L);
    int expectedWorker = 1;
    testGetInStreamFallback(workerCount, persisted, blockLocations, failedWorkers, expectedWorker);
  }

  @Test
  public void getInStreamPersistedFallbackToLeastRecentlyFailed() throws Exception {
    int workerCount = 3;
    boolean persisted = true;
    int[] blockLocations = new int[0];
    Map<Integer, Long> failedWorkers = ImmutableMap.of(
        0, 5L,
        1, 1L,
        2, 2L);
    int expectedWorker = 1;
    testGetInStreamFallback(workerCount, persisted, blockLocations, failedWorkers, expectedWorker);
  }

  @Test
  public void getInStreamInAlluxioOnlyFallbackToLeastRecentlyFailed() throws Exception {
    int workerCount = 5;
    boolean persisted = false;
    int[] blockLocations = new int[]{1, 2, 3};
    Map<Integer, Long> failedWorkers = ImmutableMap.of(
        0, 5L,
        1, 3L,
        2, 2L,
        3, 4L,
        4, 1L);
    int expectedWorker = 2;
    testGetInStreamFallback(workerCount, persisted, blockLocations, failedWorkers, expectedWorker);
  }

  @Test
  public void getInStreamInAlluxioWhenCreateStreamIsFailed() throws Exception {
    int workerCount = 5;
    boolean persisted = false;
    int[] blockLocations = new int[]{2, 3, 4};
    Map<Integer, Long> failedWorkers = ImmutableMap.of(
            0, 3L,
            1, 1L,
            3, 2L);
    int expectedWorker = 2;
    WorkerNetAddress[] workers = new WorkerNetAddress[workerCount];
    for (int i = 0; i < workers.length - 1; i++) {
      workers[i] = new WorkerNetAddress().setHost(String.format("worker-%d", i));
    }
    workers[workers.length - 1] = new WorkerNetAddress().setHost(WORKER_HOSTNAME_LOCAL);
    when(mContext.acquireBlockWorkerClient(WORKER_NET_ADDRESS_LOCAL))
        .thenThrow(new UnavailableException("failed to connect to "
            + WORKER_NET_ADDRESS_LOCAL.getHost()));
    BlockInfo info = new BlockInfo().setBlockId(BLOCK_ID)
        .setLocations(Arrays.stream(blockLocations).mapToObj(x ->
            new BlockLocation().setWorkerAddress(workers[x])).collect(Collectors.toList()));
    URIStatus dummyStatus =
        new URIStatus(new FileInfo().setPersisted(persisted)
            .setBlockIds(Collections.singletonList(BLOCK_ID))
            .setFileBlockInfos(Collections.singletonList(new FileBlockInfo().setBlockInfo(info))));
    BlockLocationPolicy mockPolicy = mock(BlockLocationPolicy.class);
    when(mockPolicy.getWorker(any())).thenAnswer(arg -> arg
        .getArgumentAt(0, GetWorkerOptions.class).getBlockWorkerInfos().iterator().next()
        .getNetAddress());
    InStreamOptions options =
        new InStreamOptions(dummyStatus, FileSystemOptions.openFileDefaults(sConf), sConf);
    options.setUfsReadLocationPolicy(mockPolicy);
    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(info);
    when(mMasterClient.getWorkerInfoList()).thenReturn(Arrays.stream(workers)
        .map(x -> new WorkerInfo().setAddress(x)).collect((Collectors.toList())));
    Map<WorkerNetAddress, Long> failedWorkerAddresses = failedWorkers.entrySet().stream()
        .map(x -> new AbstractMap.SimpleImmutableEntry<>(workers[x.getKey()], x.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    BlockInStream inStream = null;
    int i = 2;
    while (i-- > 0) {
      try {
        inStream = mBlockStore.getInStream(BLOCK_ID, options,
                failedWorkerAddresses);
      } catch (Exception e) {
        //do nothing
      }
    }
    assertEquals(workers[expectedWorker], inStream.getAddress());
  }

  private void testGetInStreamFallback(int workerCount, boolean isPersisted, int[] blockLocations,
        Map<Integer, Long> failedWorkers, int expectedWorker) throws Exception {
    WorkerNetAddress[] workers = new WorkerNetAddress[workerCount];
    Arrays.setAll(workers, i -> new WorkerNetAddress().setHost(String.format("worker-%d", i)));
    BlockInfo info = new BlockInfo().setBlockId(BLOCK_ID)
        .setLocations(Arrays.stream(blockLocations).mapToObj(x ->
            new BlockLocation().setWorkerAddress(workers[x])).collect(Collectors.toList()));
    URIStatus dummyStatus =
        new URIStatus(new FileInfo().setPersisted(isPersisted)
            .setBlockIds(Collections.singletonList(BLOCK_ID))
            .setFileBlockInfos(Collections.singletonList(new FileBlockInfo().setBlockInfo(info))));
    BlockLocationPolicy mockPolicy = mock(BlockLocationPolicy.class);
    when(mockPolicy.getWorker(any())).thenAnswer(arg -> arg
        .getArgumentAt(0, GetWorkerOptions.class).getBlockWorkerInfos().iterator().next()
        .getNetAddress());
    InStreamOptions options =
        new InStreamOptions(dummyStatus, FileSystemOptions.openFileDefaults(sConf), sConf);
    options.setUfsReadLocationPolicy(mockPolicy);
    when(mMasterClient.getBlockInfo(BLOCK_ID)).thenReturn(info);
    when(mMasterClient.getWorkerInfoList()).thenReturn(Arrays.stream(workers)
        .map(x -> new WorkerInfo().setAddress(x)).collect((Collectors.toList())));
    Map<WorkerNetAddress, Long> failedWorkerAddresses = failedWorkers.entrySet().stream()
        .map(x -> new AbstractMap.SimpleImmutableEntry<>(workers[x.getKey()], x.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    BlockInStream inStream = mBlockStore.getInStream(BLOCK_ID, options, failedWorkerAddresses);

    assertEquals(workers[expectedWorker], inStream.getAddress());
  }
}
