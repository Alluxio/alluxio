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

package alluxio.master.file.meta.cross.cluster;

import static org.mockito.ArgumentMatchers.any;

import alluxio.AlluxioURI;
import alluxio.client.cross.cluster.CrossClusterClientContextBuilder;
import alluxio.client.cross.cluster.RetryHandlingCrossClusterMasterClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.AlluxioConfiguration;
import alluxio.file.options.DescendantType;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.NetAddress;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.UfsInfo;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.proto.journal.CrossCluster.MountList;
import alluxio.proto.journal.CrossCluster.RemovedMount;

import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemCrossCluster.Factory.class, CrossClusterMount.class,
    CrossClusterClientContextBuilder.class, RetryHandlingCrossClusterMasterClient.class,
    CrossClusterMasterState.class, FileSystemContext.class})
public class CrossClusterMountTest {

  private InvalidationSyncCache mCache;
  private ArrayList<StreamObserver<PathInvalidation>> mCreatedStreams;
  private ArrayList<StreamObserver<PathInvalidation>> mCancelledStreams;
  private CrossClusterMount mCrossClusterMount;

  private List<MountSyncAddress> toMountSyncAddress(
      List<StreamObserver<PathInvalidation>> list) {
    return list.stream().map((nxt) -> ((InvalidationStream) nxt)
        .getMountSyncAddress()).collect(Collectors.toList());
  }

  private List<MountSyncAddress> toMountSyncAddress(MountList list) throws UnknownHostException {
    InetSocketAddress[] addresses = GrpcUtils.netAddressToSocketAddress(list.getAddressesList());
    return list.getMountsList().stream().map((info)
        -> new MountSyncAddress(new MountSync(list.getClusterId(),
        info.getUri()), addresses)).collect(Collectors.toList());
  }

  private Set<Set<InetSocketAddress>> toAddressSet(InetSocketAddress[] ... addresses) {
    return Arrays.stream(addresses).map(addrArray -> Arrays.stream(addrArray)
        .collect(Collectors.toSet())).collect(Collectors.toSet());
  }

  @Before
  public void before() throws Exception {

    PowerMockito.mockStatic(FileSystemCrossCluster.Factory.class);
    Mockito.when(FileSystemCrossCluster.Factory.create(any(FileSystemContext.class)))
        .thenReturn(Mockito.mock(FileSystemCrossCluster.class));
    PowerMockito.mockStatic(FileSystemContext.class);
    Mockito.when(FileSystemContext.create(any(AlluxioConfiguration.class), any()))
        .thenReturn(Mockito.mock(FileSystemContext.class));
    // mock the client connections so that subscribing to an invalidation stream
    // calls subscribe invalidations on the target master directly
    CrossClusterConnections mockConnections = Mockito.spy(new CrossClusterConnections());
    PowerMockito.whenNew(CrossClusterConnections.class).withAnyArguments()
        .thenReturn(mockConnections);
    Mockito.doAnswer(mock -> {
      InvalidationStream stream = mock.getArgument(1);
      mCreatedStreams.add(stream);
      mock.callRealMethod();
      return null;
    }).when(mockConnections).addStream(any(), any());
    // Track cancelled streams through mocking
    mCancelledStreams = new ArrayList<>();
    PowerMockito.whenNew(InvalidationStream.class).withArguments(any(MountSyncAddress.class),
            any(InvalidationSyncCache.class), any(CrossClusterMount.class))
        .then(invocation -> {
          InvalidationStream stream = Mockito.mock(InvalidationStream.class, Mockito.withSettings()
              .useConstructor(invocation.getArguments()).defaultAnswer(Mockito.CALLS_REAL_METHODS));
          Mockito.doAnswer(mock -> {
            mCancelledStreams.add(stream);
            return null;
          }).when(stream).cancel();
          return stream;
        });

    mCache = new InvalidationSyncCache((ufsPath) ->
      Optional.of(new AlluxioURI(ufsPath.toString().replace("s3:/", ""))));
    mCreatedStreams = new ArrayList<>();
    mCrossClusterMount = new CrossClusterMount("c1",
        mCache, stream -> { },
        (stream) -> { });
  }

  private MountInfo createMountInfo(String alluxioPath, String ufsPath, long mountId) {
    return createMountInfo(alluxioPath, ufsPath, mountId, false, true);
  }

  private MountInfo createMountInfo(
      String alluxioPath, String ufsPath, long mountId, boolean readOnly, boolean crossCluster) {
    return new MountInfo(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath), mountId,
        MountPOptions.newBuilder().setCrossCluster(crossCluster).setReadOnly(readOnly).build());
  }

  private MountList.Builder buildMountList(String clusterId, InetSocketAddress[] addresses,
                                   Collection<String> mountPaths) {
    return MountList.newBuilder()
        .setClusterId(clusterId)
        .addAllAddresses(Arrays.stream(addresses).map((address) ->
            NetAddress.newBuilder().setHost(address.getHostName())
                .setRpcPort(address.getPort()).build()).collect(Collectors.toList()))
        .addAllMounts(mountPaths.stream().map((path)
                -> UfsInfo.newBuilder().setUri(path).setProperties(
                MountPOptions.newBuilder().setCrossCluster(true).build()).build())
            .collect(Collectors.toList()));
  }

  @Test
  public void NonCrossClusterMountTest() throws UnknownHostException {
    // create a local ufs mount without cross cluster enabled
    MountInfo rootUfs = createMountInfo("/", "s3://some-bucket", 1, false, false);
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2, but with cross cluster enabled
    // be sure no streams are created
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
        }));
    c2MountState.addMount(createMountInfo("/", "s3://some-bucket", 1));
    Assert.assertEquals(Collections.emptyList(), toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(Collections.emptyList(), toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void MountIntersectionTest() throws UnknownHostException {
    // create a local ufs mount
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();
    MountInfo rootUfs = createMountInfo("/", "s3://some-bucket", 1);
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    c2MountState.addMount(createMountInfo("/", "s3://some-bucket", 1));
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>(mountSync);
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // add another ufs mount to c2 that is not intersecting
    c2MountState.addMount(createMountInfo("/other", "s3://other-bucket", 2));
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // create an intersecting mount at a new cluster c3
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1235)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c3MountList[0] = mountList;
        }));
    c3MountState.addMount(createMountInfo("/", "s3://some-bucket/some-folder", 1));
    mountSync = toMountSyncAddress(c3MountList[0]);
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses, c3Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void MountIntersectionSubFolderTest() throws UnknownHostException {
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // create a local mount with a nested folder
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"),
        new AlluxioURI("s3://some-bucket/some-folder"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // create a UFS mount at cluster c2 that is the parent of the local mount
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
        }));
    c2MountState.addMount(createMountInfo("/", "s3://some-bucket", 1));
    // the subscription created by the local cluster should only be of the subfolder
    List<MountSyncAddress> mountSync =
        Collections.singletonList(new MountSyncAddress(new MountSync(
            "c2", "s3://some-bucket/some-folder"), c2Addresses));
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>(mountSync);
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // add another ufs mount to c2 that is not intersecting
    c2MountState.addMount(createMountInfo("/other", "s3://other-bucket", 2));
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // create an intersecting mount at a new cluster c3 that is a subfolder of the local mount
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1235)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c3MountList[0] = mountList;
        }));
    c3MountState.addMount(createMountInfo("/",
        "s3://some-bucket/some-folder/nested-folder", 1));
    mountSync = toMountSyncAddress(c3MountList[0]);
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses, c3Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void MountIntersectionCancelTest() throws UnknownHostException {
    // add a UFS mount at the local cluster
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>(mountSync);
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // change the mount at c2 so that it no longer intersects
    // the local subscriptions should be cancelled, and the connection should be closed
    c2MountState.removeMount(c2MountInfo);
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>(mountSync);
    activeSubscriptions.clear();
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());

    // add an intersecting mount at a separate cluster, a new subscription should be created
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1235)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c3MountList[0] = mountList;
        }));
    c3MountState.addMount(createMountInfo("/", "s3://some-bucket/some-folder", 1));
    mountSync = toMountSyncAddress(c3MountList[0]);
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c3Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void readOnlyExternalMountTest() throws UnknownHostException {
    // create a local ufs mount
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>();
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>();

    MountInfo rootUfs = createMountInfo("/", "s3://some-bucket", 1);
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2, but have the mount be read only
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1, true, true);
    c2MountState.addMount(c2MountInfo);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());

    // update the mount at c2, so it is no longer read only, the local cluster should subscribe
    c2MountState.removeMount(c2MountInfo);
    c2MountInfo = createMountInfo("/", "s3://some-bucket", 1, false, true);
    c2MountState.addMount(c2MountInfo);
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    createdStreams = new ArrayList<>(mountSync);
    activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void localMountChangeTest() throws UnknownHostException {
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>();
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>();
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // Add a local mount that does not have cross cluster enabled

    // first create an external mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());

    // then add the local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);
    // ensure a stream was created
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // add another external mount, but one that does not intersect the local
    c2MountState.addMount(createMountInfo("/other", "s3://other-bucket", 2));
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // add another intersecting mount from a different cluster
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1235)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c3MountList[0] = mountList;
        }));
    c3MountState.addMount(createMountInfo("/", "s3://some-bucket/some-folder", 1));
    mountSync = toMountSyncAddress(c3MountList[0]);
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses, c3Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // remove the local mount, ensure all streams are cancelled
    mCrossClusterMount.removeLocalMount(rootUfs);
    activeSubscriptions.clear();
    cancelledStreams.addAll(createdStreams);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(new HashSet<>(cancelledStreams),
        new HashSet<>(toMountSyncAddress(mCancelledStreams)));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void StreamCompletedTest() throws UnknownHostException {
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // add a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // add an intersecting external mount
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    c2MountState.addMount(createMountInfo("/", "s3://some-bucket", 1));
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>(mountSync);
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // complete the stream, a new stream should be created for the same mount info
    mCreatedStreams.get(0).onCompleted();
    createdStreams.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // complete the stream by an error, a new stream should be created with the same mount info
    mCreatedStreams.get(1).onError(new Throwable());
    createdStreams.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void MountIntersectionChangeAddressTest() throws UnknownHostException {
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // mount a UFS path at the local cluster
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // mount an intersecting ufs path at c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    c2MountState.addMount(createMountInfo("/", "s3://some-bucket", 1));
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>(mountSync);
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // change the address of c2, old streams should be cancelled and new streams should be created
    // new connections should be made
    c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1236)};
    c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    c2MountState.addMount(createMountInfo("/", "s3://some-bucket", 1));
    cancelledStreams.addAll(mountSync); // all the previous streams to c2 should be cancelled
    mountSync = toMountSyncAddress(c2MountList[0]);
    createdStreams.addAll(mountSync);
    activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());
  }

  @Test
  public void MountRemovalTest() throws UnknownHostException {
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>();
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>();
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // first ensure the local path is synced
    String mountPath = "/some-bucket";
    String ufsMountPath = "s3:/" + mountPath;
    mCache.notifySyncedPath(new AlluxioURI(mountPath), DescendantType.ALL,
        mCache.startSync(new AlluxioURI(mountPath)), null);

    // now add and remove the ufs mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          try {
            mCrossClusterMount.setExternalMountList(mountList);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    c2MountState.removeMount(c2MountInfo);
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());

    // create a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());

    // update the removal time of the external mount list, so that an invalidation will happen
    mCrossClusterMount.setExternalMountList(MountList.newBuilder().mergeFrom(
        c2MountList[0]).addAllRemovedMounts(Collections.emptyList())
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsMountPath)
            .setTime(System.currentTimeMillis() + 1).build()).build());
    // ensure the path needs synchronization
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE).isShouldSync());

    // change the c2 mount so the path is no longer removed
    c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    // ensure the streams are created at the local cluster
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(toAddressSet(c2Addresses),
        mCrossClusterMount.getConnections().getClients().keySet());

    // remove the path again from c2
    c2MountState.removeMount(c2MountInfo);
    activeSubscriptions.clear();
    cancelledStreams.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());
    // ensure a sync is needed at the local path
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE).isShouldSync());

    // let there be a nested removed mount at c2 (insert manually)
    String removePath = "/some-bucket/nested";
    String ufsRemovePath = "s3:/" + removePath;
    // first ensure the path is synced
    mCache.notifySyncedPath(new AlluxioURI(removePath), DescendantType.ALL,
        mCache.startSync(new AlluxioURI(removePath)), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE).isShouldSync());
    MountList mountListNext = MountList.newBuilder().mergeFrom(c2MountList[0])
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsRemovePath)
            .setTime(2).build())
        .build();
    mCrossClusterMount.setExternalMountList(mountListNext);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());
    // after the removal the path should need to be synced
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE).isShouldSync());

    // send an old removal timestamp, this should not trigger a new sync
    mCache.notifySyncedPath(new AlluxioURI(removePath), DescendantType.ALL,
        mCache.startSync(new AlluxioURI(removePath)), null);
    mountListNext = MountList.newBuilder().mergeFrom(c2MountList[0])
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsRemovePath)
            .setTime(1).build())
        .build();
    mCrossClusterMount.setExternalMountList(mountListNext);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE).isShouldSync());

    // sync again the mounted path
    mCache.notifySyncedPath(new AlluxioURI(mountPath), DescendantType.ALL,
        mCache.startSync(new AlluxioURI(mountPath)), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE).isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE).isShouldSync());

    // again (manually) update the nested removal timestamp, it should need a sync
    // but also (manually) update the removal timestamp for the root path,
    // but use and old timestamp, so it should not need a sync
    mountListNext = buildMountList("c2", c2Addresses,
        Collections.emptyList())
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsRemovePath)
            .setTime(3).build())
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsMountPath)
            .setTime(1).build())
        .build();
    mCrossClusterMount.setExternalMountList(mountListNext);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    Assert.assertEquals(Collections.emptySet(),
        mCrossClusterMount.getConnections().getClients().keySet());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE).isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE).isShouldSync());
  }
}
