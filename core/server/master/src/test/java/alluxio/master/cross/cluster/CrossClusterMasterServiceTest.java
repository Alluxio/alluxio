package alluxio.master.cross.cluster;

import static org.mockito.ArgumentMatchers.any;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.client.cross.cluster.CrossClusterClient;
import alluxio.client.cross.cluster.TestingCrossClusterFileSystem;
import alluxio.client.cross.cluster.TestingCrossClusterMasterClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.MountList;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.NetAddress;
import alluxio.master.MasterTestUtils;
import alluxio.master.file.meta.cross.cluster.CrossClusterInvalidationStream;
import alluxio.master.file.meta.cross.cluster.CrossClusterMount;
import alluxio.master.file.meta.cross.cluster.CrossClusterMountClientRunner;
import alluxio.master.file.meta.cross.cluster.CrossClusterMountSubscriber;
import alluxio.master.file.meta.cross.cluster.InvalidationSyncCache;
import alluxio.master.file.meta.cross.cluster.LocalMountState;
import alluxio.master.file.meta.cross.cluster.MountSync;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemCrossCluster.Factory.class, FileSystemContext.class})
public class CrossClusterMasterServiceTest {

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, Object>() {
        {
          put(PropertyKey.USER_CONF_CLUSTER_DEFAULT_ENABLED, false);
          put(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true);
          put(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, "host:1000");
        }
      }, Configuration.modifiableGlobal());

  @Rule
  public final GrpcCleanupRule mGrpcCleanup = new GrpcCleanupRule();

  ArrayList<ManagedChannel> mChannels;
  CrossClusterClient mClient;
  CrossClusterMountClientRunner mClientRunner;
  CrossClusterMountSubscriber mClientSubscriber;
  CrossClusterMount mMount;
  DefaultCrossClusterMaster mMaster;
  LocalMountState mLocalMountState;
  InetSocketAddress[] mAddresses;
  InetSocketAddress[] mAddresses2;
  InetSocketAddress[] mAddresses3;
  String mClusterId;
  Server mServer;
  String mServerName;
  TestingFileSystemMasterClientServiceHandler mFsMaster;

  @Before
  public void before() throws Exception {
    mChannels = new ArrayList<>();
    mMaster = new DefaultCrossClusterMaster(
        MasterTestUtils.testMasterContext());
    mFsMaster = new TestingFileSystemMasterClientServiceHandler();
    mServerName = InProcessServerBuilder.generateName();

    PowerMockito.mockStatic(FileSystemContext.class);
    Mockito.when(FileSystemContext.create(any(AlluxioConfiguration.class), any()))
        .thenReturn(Mockito.mock(FileSystemContext.class));
    PowerMockito.mockStatic(FileSystemCrossCluster.Factory.class);
    Mockito.when(FileSystemCrossCluster.Factory.create(any(FileSystemContext.class)))
            .then(mock -> new TestingCrossClusterFileSystem(mGrpcCleanup.register(
                InProcessChannelBuilder.forName(mServerName).directExecutor().build())));

    restartServer();

    mClient = new TestingCrossClusterMasterClient(mGrpcCleanup.register(
        InProcessChannelBuilder.forName(mServerName).directExecutor().build()));
    mClusterId = "c1";
    mClientRunner = new CrossClusterMountClientRunner(mClient);
    mMount = new CrossClusterMount(mClusterId, new InvalidationSyncCache(
        uri -> Optional.of(new AlluxioURI("reverse-resolve:" + uri.toString()))),
        (ignored) -> { }, (ignored) -> { });
    mClientSubscriber = new CrossClusterMountSubscriber("c1", mClient, mMount);

    mAddresses = new InetSocketAddress[]{ new InetSocketAddress("localhost", 1234)};
    mAddresses2 = new InetSocketAddress[]{ new InetSocketAddress("localhost", 1235)};
    mAddresses3 = new InetSocketAddress[]{ new InetSocketAddress("localhost", 1236)};
    mLocalMountState = new LocalMountState(mClusterId, mAddresses,
        mClientRunner::onLocalMountChange);
  }

  private void restartServer() throws Exception {
    if (mServer != null) {
      mServer.shutdownNow();
      mServer.awaitTermination();
    }
    mServer = InProcessServerBuilder.forName(mServerName).directExecutor().addService(
        new CrossClusterMasterClientServiceHandler(mMaster))
        .addService(mFsMaster).build();
    mGrpcCleanup.register(mServer.start());
  }

  @After
  public void after() throws Exception {
    mMount.close();
    mClientRunner.close();
    mClientSubscriber.close();
    mClient.close();
  }

  private void start() {
    mClientRunner.start();
    mClientSubscriber.start();
  }

  private void stop() {
    mClientRunner.stop();
    mClientSubscriber.stop();
  }

  private List<MountSync> toMountSync(String clusterId, MountInfo ... mounts) {
    return Arrays.stream(mounts).map(nxt -> MountSync.fromMountInfo(clusterId, nxt))
        .collect(Collectors.toList());
  }

  private MountList toMountList(String clusterId, InetSocketAddress[] addresses,
                                MountInfo ... mounts) {
    MountList.Builder builder = MountList.newBuilder();
    builder.setClusterId(clusterId);
    builder.addAllMounts(Arrays.stream(mounts).map(
        MountInfo::toUfsInfo).collect(Collectors.toList()));
    builder.addAllAddresses(Arrays.stream(addresses).map(address ->
        NetAddress.newBuilder().setHost(address.getHostName())
            .setRpcPort(address.getPort()).build()).collect(Collectors.toList()));
    return builder.build();
  }

  @Test
  public void subscribeMountTest() throws Exception {
    start();
    // there should be a stream to the configuration service
    CommonUtils.waitFor("Created stream to configuration service",
        () -> mMaster.getCrossClusterState().getStreams().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));

    // create a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mMount.addLocalMount(rootUfs);
    // this should update the configuration service
    mLocalMountState.addMount(rootUfs);
    CommonUtils.waitFor("Updated state at server",
        () -> mMaster.getCrossClusterState().getMounts().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountList(mClusterId, mAddresses, rootUfs),
        mMaster.getCrossClusterState().getMounts().get(mClusterId));

    // add an external mount at the configuration service
    mMaster.setMountList(toMountList("c2", mAddresses2, rootUfs));
    // the local cluster should have started an invalidation stream subscription
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));

    // close the stream, ensure there is a new one created
    mFsMaster.getStreams().get(0).onError(new IOException("Some error"));
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 2,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));

    // close the stream to the configuration service, ensure we can still get updated
    mMaster.getCrossClusterState().getStreams().get(mClusterId).onError(
        new IOException("Some error"));
    // add an external mount at the configuration service
    mMaster.setMountList(toMountList("c3", mAddresses3, rootUfs));
    CommonUtils.waitFor("Reconnection to configuration service",
        () -> new HashSet<>(mMount.getExternalMountsMap().keySet())
            .equals(new HashSet<>(Arrays.asList("c2", "c3"))),
        WaitForOptions.defaults().setTimeoutMs(5000));
  }

  @Test
  public void serverResetTest() throws Exception {
    start();
    // there should be a stream to the configuration service
    CommonUtils.waitFor("Created stream to configuration service",
        () -> mMaster.getCrossClusterState().getStreams().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));

    // create a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mMount.addLocalMount(rootUfs);
    // this should update the configuration service
    mLocalMountState.addMount(rootUfs);

    // add an external mount at the configuration service
    mMaster.setMountList(toMountList("c2", mAddresses2, rootUfs));
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));

    // restart the servers
    restartServer();
    // the local cluster should have started new streams to the servers
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 2,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));

    // be sure we can get updates for a new mount
    mMaster.setMountList(toMountList("c3", mAddresses3, rootUfs));
    CommonUtils.waitFor("Reconnection to configuration service",
        () -> new HashSet<>(mMount.getExternalMountsMap().keySet())
            .equals(new HashSet<>(Arrays.asList("c2", "c3"))),
        WaitForOptions.defaults().setTimeoutMs(5000));
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 3,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs, rootUfs, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));
  }

  @Test
  public void clientChangeTest() throws Exception {
    start();
    // there should be a stream to the configuration service
    CommonUtils.waitFor("Created stream to configuration service",
        () -> mMaster.getCrossClusterState().getStreams().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));

    // create a local mount
    MountInfo rootUfs = new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3://some-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mMount.addLocalMount(rootUfs);
    // this should update the configuration service
    mLocalMountState.addMount(rootUfs);

    // add an external mount at the configuration service
    mMaster.setMountList(toMountList("c2", mAddresses2, rootUfs));
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));

    // change the server address, and restart it
    mClient.close();
    mServerName = InProcessServerBuilder.generateName();
    restartServer();
    // update the mount list
    MountInfo newUfs = new MountInfo(new AlluxioURI("/other"), new AlluxioURI("s3://other-bucket"),
        1, MountPOptions.newBuilder().setCrossCluster(true).build());
    mMount.addLocalMount(newUfs);
    mLocalMountState.addMount(newUfs);

    // change the client, so the new mount list can be uploaded to the new server address
    CrossClusterClient newClient = new TestingCrossClusterMasterClient(mGrpcCleanup.register(
        InProcessChannelBuilder.forName(mServerName).directExecutor().build()));
    mClientRunner.changeClient(newClient);
    // be sure the new mount list is uploaded to the config service
    CommonUtils.waitFor("Updated mount list at new server address",
        () -> toMountList(mClusterId, mAddresses, rootUfs, newUfs).equals(
        mMaster.getCrossClusterState().getMounts().get(mClusterId)),
        WaitForOptions.defaults().setTimeoutMs(5000));

    // change the client at subscriber, so it should create a new subscription
    // to the new address
    mClientSubscriber.changeClient(newClient);
    // change the addresses of c2, so that we are sure we get the new updates
    mMaster.setMountList(toMountList("c2", mAddresses3, rootUfs));
    CommonUtils.waitFor("Invalidation stream subscription",
        () -> mFsMaster.getStreams().size() == 2,
        WaitForOptions.defaults().setTimeoutMs(5000));
    Assert.assertEquals(toMountSync(mClusterId, rootUfs, rootUfs),
        mFsMaster.getStreams().stream().map(CrossClusterInvalidationStream::getMountSync)
            .collect(Collectors.toList()));
  }
}
