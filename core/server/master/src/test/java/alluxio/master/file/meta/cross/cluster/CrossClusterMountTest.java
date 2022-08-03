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

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.grpc.MountList;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.NetAddress;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.RemovedMount;
import alluxio.grpc.UfsInfo;
import alluxio.master.file.meta.options.MountInfo;

import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CrossClusterMountTest {

  private InvalidationSyncCache mCache;
  private ArrayList<StreamObserver<PathInvalidation>> mCreatedStreams;
  private ArrayList<StreamObserver<PathInvalidation>> mCancelledStreams;
  private CrossClusterMount mCrossClusterMount;

  private List<MountSyncAddress> toMountSyncAddress(
      List<StreamObserver<PathInvalidation>> list) {
    return list.stream().map((nxt) -> ((CrossClusterMount.InvalidationStream) nxt)
        .getMountSyncAddress()).collect(Collectors.toList());
  }

  private List<MountSyncAddress> toMountSyncAddress(MountList list) {
    return list.getMountsList().stream().map((info)
        -> new MountSyncAddress(new MountSync(list.getClusterId(),
        info.getUri()), list.getAddressesList().stream().map((address) ->
            new InetSocketAddress(address.getHost(), address.getRpcPort()))
        .toArray(InetSocketAddress[]::new))).collect(Collectors.toList());
  }

  @Before
  public void before() {
    mCache = new InvalidationSyncCache((ufsPath) ->
      Optional.of(new AlluxioURI(ufsPath.toString().replace("s3:/", ""))));
    mCreatedStreams = new ArrayList<>();
    mCancelledStreams = new ArrayList<>();
    mCrossClusterMount = new CrossClusterMount("c1",
        mCache, (stream) -> mCreatedStreams.add(stream),
        (stream) -> mCancelledStreams.add(stream));
  }

  private MountInfo createMountInfo(String alluxioPath, String ufsPath, long mountId) {
    return createMountInfo(alluxioPath, ufsPath, mountId, false);
  }

  private MountInfo createMountInfo(
      String alluxioPath, String ufsPath, long mountId, boolean readOnly) {
    return new MountInfo(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath), mountId,
        MountPOptions.newBuilder().setCrossCluster(true).setReadOnly(readOnly).build());
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
  public void MountIntersectionTest() {
    // create a local ufs mount
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();
    MountInfo rootUfs = createMountInfo("/", "s3://some-bucket", 1);
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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

    // add another ufs mount to c2 that is not intersecting
    c2MountState.addMount(createMountInfo("/other", "s3://other-bucket", 2));
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // create an intersecting mount at a new cluster c3
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host2", 1234)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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
  }

  @Test
  public void MountIntersectionSubFolderTest() {
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // create a local mount with a nested folder
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"),
        new AlluxioURI("s3://some-bucket/some-folder"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // create a ufs mount at cluster c2 that is the parent of the local mount
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> mCrossClusterMount.setExternalMountList(mountList)));
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

    // add another ufs mount to c2 that is not intersecting
    c2MountState.addMount(createMountInfo("/other", "s3://other-bucket", 2));
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // create an intersecting mount at a new cluster c3 that is a subfolder of the local mount
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host2", 1234)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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
  }

  @Test
  public void MountIntersectionCancelTest() {
    // add a ufs mount at the local cluster
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>(mountSync);
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>(mountSync);

    // change the mount at c2 so that it no longer intersects
    // the local subscriptions should be cancelled
    c2MountState.removeMount(c2MountInfo);
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>(mountSync);
    activeSubscriptions.clear();
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // add an intersecting mount at a separate cluster, a new subscription should be created
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host2", 1234)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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
  }

  @Test
  public void readOnlyExternalMountTest() {
    // create a local ufs mount
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>();
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>();

    MountInfo rootUfs = createMountInfo("/", "s3://some-bucket", 1);
    mCrossClusterMount.addLocalMount(rootUfs);

    // create the same ufs mount at cluster c2, but have the mount be read only
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1, true);
    c2MountState.addMount(c2MountInfo);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // update the mount at c2, so it is no longer read only, the local cluster should subscribe
    c2MountState.removeMount(c2MountInfo);
    c2MountInfo = createMountInfo("/", "s3://some-bucket", 1, false);
    c2MountState.addMount(c2MountInfo);
    List<MountSyncAddress> mountSync = toMountSyncAddress(c2MountList[0]);
    createdStreams = new ArrayList<>(mountSync);
    activeSubscriptions = new HashSet<>(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
  }

  @Test
  public void localMountChangeTest() {
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>();
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>();
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // first create an external mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

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

    // add another external mount, but one that does not intersect the local
    c2MountState.addMount(createMountInfo("/other", "s3://other-bucket", 2));
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // add another intersecting mount from a different cluster
    MountList[] c3MountList = new MountList[] {null};
    InetSocketAddress[] c3Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host2", 1234)};
    LocalMountState c3MountState = new LocalMountState("c3", c3Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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

    // remove the local mount, ensure all streams are cancelled
    mCrossClusterMount.removeLocalMount(rootUfs);
    activeSubscriptions.clear();
    cancelledStreams.addAll(createdStreams);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(new HashSet<>(cancelledStreams),
        new HashSet<>(toMountSyncAddress(mCancelledStreams)));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
  }

  @Test
  public void StreamCompletedTest() {
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // add a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // add an intersecting external mount
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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

    // complete the stream, a new stream should be created for the same mount info
    mCreatedStreams.get(0).onCompleted();
    createdStreams.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // complete the stream by an error, a new stream should be created with the same mount info
    mCreatedStreams.get(1).onError(new Throwable());
    createdStreams.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
  }

  @Test
  public void MountIntersectionChangeAddressTest() {
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // mount a ufs path at the local cluster
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    // mount an intersecting ufs path at c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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

    // change the address of c2, old streams should be cancelled and new streams should be created
    c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host.new", 1234)};
    c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
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
  }

  @Test
  public void MountRemovalTest() {
    ArrayList<MountSyncAddress> createdStreams = new ArrayList<>();
    Set<MountSyncAddress> activeSubscriptions = new HashSet<>();
    ArrayList<MountSyncAddress> cancelledStreams = new ArrayList<>();

    // first ensure the local path is synced
    String mountPath = "/some-bucket";
    String ufsMountPath = "s3:/" + mountPath;
    mCache.startSync(new AlluxioURI(mountPath));
    mCache.notifySyncedPath(new AlluxioURI(mountPath), DescendantType.ALL);

    // now add and remove the ufs mount at cluster c2
    MountList[] c2MountList = new MountList[] {null};
    InetSocketAddress[] c2Addresses = new InetSocketAddress[] {
        new InetSocketAddress("other.host", 1234)};
    LocalMountState c2MountState = new LocalMountState("c2", c2Addresses,
        (mountList -> {
          mCrossClusterMount.setExternalMountList(mountList);
          c2MountList[0] = mountList;
        }));
    MountInfo c2MountInfo = createMountInfo("/", "s3://some-bucket", 1);
    c2MountState.addMount(c2MountInfo);
    c2MountState.removeMount(c2MountInfo);

    // create a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    // update the removal time of the external mount list, so that an invalidation will happen
    mCrossClusterMount.setExternalMountList(MountList.newBuilder().mergeFrom(
        c2MountList[0]).addAllRemovedMounts(Collections.emptyList())
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsMountPath)
            .setTime(System.currentTimeMillis() + 1).build()).build());
    // ensure the path needs synchronization
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE));

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

    // remove the path again from c2
    c2MountState.removeMount(c2MountInfo);
    activeSubscriptions.clear();
    cancelledStreams.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    // ensure a sync is needed at the local path
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE));

    // let there be a nested removed mount at c2 (insert manually)
    String removePath = "/some-bucket/nested";
    String ufsRemovePath = "s3:/" + removePath;
    // first ensure the path is synced
    mCache.startSync(new AlluxioURI(removePath));
    mCache.notifySyncedPath(new AlluxioURI(removePath), DescendantType.ALL);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE));
    MountList mountListNext = MountList.newBuilder().mergeFrom(c2MountList[0])
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsRemovePath)
            .setTime(2).build())
        .build();
    mCrossClusterMount.setExternalMountList(mountListNext);
    Assert.assertEquals(createdStreams, toMountSyncAddress(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSyncAddress(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
    // after the removal the path should need to be synced
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE));

    // send an old removal timestamp, this should not trigger a new sync
    mCache.startSync(new AlluxioURI(removePath));
    mCache.notifySyncedPath(new AlluxioURI(removePath), DescendantType.ALL);
    mountListNext = MountList.newBuilder().mergeFrom(c2MountList[0])
        .addRemovedMounts(RemovedMount.newBuilder().setUfsPath(ufsRemovePath)
            .setTime(1).build())
        .build();
    mCrossClusterMount.setExternalMountList(mountListNext);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE));

    // sync again the mounted path
    mCache.startSync(new AlluxioURI(mountPath));
    mCache.notifySyncedPath(new AlluxioURI(mountPath), DescendantType.ALL);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE));

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
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI(mountPath),
        0, DescendantType.NONE));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(removePath),
        0, DescendantType.NONE));
  }
}
