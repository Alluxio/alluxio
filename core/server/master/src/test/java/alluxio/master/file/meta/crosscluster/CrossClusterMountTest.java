package alluxio.master.file.meta.crosscluster;

import alluxio.AlluxioURI;
import alluxio.grpc.MountList;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.UfsInfo;
import alluxio.master.file.meta.options.MountInfo;

import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CrossClusterMountTest {

  private InvalidationSyncCache mCache;
  private ArrayList<StreamObserver<PathInvalidation>> mCreatedStreams;
  private ArrayList<StreamObserver<PathInvalidation>> mCancelledStreams;
  private CrossClusterMount mCrossClusterMount;

  private List<CrossClusterMount.MountSync> toMountSync(
      List<StreamObserver<PathInvalidation>> list) {
    return list.stream().map((nxt) -> ((CrossClusterMount.InvalidationStream) nxt)
        .getMountSync()).collect(Collectors.toList());
  }

  private List<CrossClusterMount.MountSync> toMountSync(MountList list) {
    return list.getMountsList().stream().map((info)
        -> new CrossClusterMount.MountSync(list.getClusterId(),
        info.getUri())).collect(Collectors.toList());
  }

  @Before
  public void before() {
    mCache = new InvalidationSyncCache();
    mCreatedStreams = new ArrayList<>();
    mCancelledStreams = new ArrayList<>();
    mCrossClusterMount = new CrossClusterMount("c1",
        mCache, (stream) -> mCreatedStreams.add(stream),
        (stream) -> mCancelledStreams.add(stream));
  }

  private MountList buildMountList(String clusterId, Collection<String> mountPaths) {
    return MountList.newBuilder()
        .setClusterId(clusterId)
        .addAllMounts(mountPaths.stream().map((path)
                -> UfsInfo.newBuilder().setUri(path).setProperties(
                MountPOptions.newBuilder().setCrossCluster(true).build()).build())
            .collect(Collectors.toList())).build();
  }

  @Test
  public void MountIntersectionTest() {
    ArrayList<CrossClusterMount.MountSync> createdStreams = new ArrayList<>();
    ArrayList<CrossClusterMount.MountSync> cancelledStreams = new ArrayList<>();
    Set<CrossClusterMount.MountSync> activeSubscriptions = new HashSet<>();

    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mCrossClusterMount.addLocalMount(rootUfs);

    MountList mountList = buildMountList("c2", Collections.singleton("s3://some-bucket"));
    List<CrossClusterMount.MountSync> mountSync = toMountSync(mountList);
    mCrossClusterMount.setExternalMountList(mountList);
    createdStreams.addAll(mountSync);
    activeSubscriptions.addAll(mountSync);
    Assert.assertEquals(createdStreams, toMountSync(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSync(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());

    mountList = buildMountList("c2", Arrays.asList("s3://some-bucket", "s3://other-bucket"));
    mCrossClusterMount.setExternalMountList(mountList);
    Assert.assertEquals(createdStreams, toMountSync(mCreatedStreams));
    Assert.assertEquals(cancelledStreams, toMountSync(mCancelledStreams));
    Assert.assertEquals(activeSubscriptions,
        mCrossClusterMount.getActiveSubscriptions());
  }
}
