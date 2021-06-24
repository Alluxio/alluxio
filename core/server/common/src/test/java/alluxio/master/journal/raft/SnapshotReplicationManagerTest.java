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

package alluxio.master.journal.raft;

import static org.mockito.ArgumentMatchers.any;

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.JournalQueryRequest;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class SnapshotReplicationManagerTest {
  private static final int SNAPSHOT_SIZE = 100000;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_CHUNK_SIZE,
          "32KB", ServerConfiguration.global());

  private WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(30000);
  private SnapshotReplicationManager mLeaderSnapshotManager;
  private SnapshotReplicationManager mFollowerSnapshotManager;
  private RaftJournalSystem mLeader;
  private RaftJournalSystem mFollower;
  private SimpleStateMachineStorage mLeaderStore;
  private SimpleStateMachineStorage mFollowerStore;
  private Server mServer;

  @Before
  public void before() throws Exception {
    mLeader = Mockito.mock(RaftJournalSystem.class);
    Mockito.when(mLeader.isLeader()).thenReturn(true);
    Mockito.when(mLeader.getLocalPeerId()).thenReturn(RaftPeerId.getRaftPeerId("leader"));
    Mockito.when(mLeader.getQuorumServerInfoList()).thenReturn(
        Collections.singletonList(QuorumServerInfo.newBuilder()
            .setServerAddress(NetAddress.newBuilder()
                .setHost("follower").setRpcPort(12345)).build()));
    Mockito.when(mLeader.sendMessageAsync(any(), any())).thenAnswer((args) -> {
      Message message = args.getArgument(1, Message.class);
      JournalQueryRequest queryRequest = JournalQueryRequest.parseFrom(
          message.getContent().asReadOnlyByteBuffer());
      Message response = mFollowerSnapshotManager.handleRequest(queryRequest);
      RaftClientReply reply = Mockito.mock(RaftClientReply.class);
      Mockito.when(reply.getMessage()).thenReturn(response);
      return CompletableFuture.completedFuture(reply);
    });
    mLeaderStore = getSimpleStateMachineStorage();
    mLeaderSnapshotManager = new SnapshotReplicationManager(mLeader, mLeaderStore);

    String serverName = InProcessServerBuilder.generateName();
    mServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new RaftJournalServiceHandler(mLeaderSnapshotManager)).build();
    mServer.start();
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    RaftJournalServiceGrpc.RaftJournalServiceStub stub = RaftJournalServiceGrpc.newStub(channel);
    RaftJournalServiceClient client = Mockito.mock(RaftJournalServiceClient.class);

    // download rpc mock
    Mockito.when(client.downloadSnapshot(any())).thenAnswer((args) -> {
      StreamObserver responseObserver = args.getArgument(0, StreamObserver.class);
      return stub.downloadSnapshot(responseObserver);
    });

    // upload rpc mock
    Mockito.when(client.uploadSnapshot(any())).thenAnswer((args) -> {
      StreamObserver responseObserver = args.getArgument(0, StreamObserver.class);
      return stub.uploadSnapshot(responseObserver);
    });

    mFollowerStore = getSimpleStateMachineStorage();
    mFollower = Mockito.mock(RaftJournalSystem.class);
    mFollowerSnapshotManager = new SnapshotReplicationManager(mFollower, mFollowerStore, client);
  }

  private SimpleStateMachineStorage getSimpleStateMachineStorage() throws IOException {
    RaftStorage rs = new RaftStorageImpl(mFolder.newFolder(CommonUtils.randomAlphaNumString(6)),
            RaftServerConfigKeys.Log.CorruptionPolicy.getDefault());
    SimpleStateMachineStorage snapshotStore = new SimpleStateMachineStorage();
    snapshotStore.init(rs);
    return snapshotStore;
  }

  private void createSnapshotFile(SimpleStateMachineStorage storage) throws IOException {
    java.io.File file = storage.getSnapshotFile(0, 1);
    FileUtils.writeByteArrayToFile(file, BufferUtils.getIncreasingByteArray(SNAPSHOT_SIZE));
    storage.loadLatestSnapshot();
  }

  private void validateSnapshotFile(SimpleStateMachineStorage storage) throws IOException {
    SingleFileSnapshotInfo snapshot = storage.getLatestSnapshot();
    Assert.assertNotNull(snapshot);
    Assert.assertEquals(TermIndex.valueOf(0, 1), snapshot.getTermIndex());
    byte[] received = FileUtils.readFileToByteArray(snapshot.getFiles().get(0).getPath().toFile());
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SNAPSHOT_SIZE, received));
  }

  @After
  public void After() throws Exception {
    mLeaderSnapshotManager.close();
    mFollowerSnapshotManager.close();
    mServer.shutdown();
    mServer.awaitTermination();
  }

  @Test
  public void copySnapshotToLeader() throws Exception {
    createSnapshotFile(mFollowerStore);
    Assert.assertNull(mLeaderStore.getLatestSnapshot());

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    validateSnapshotFile(mLeaderStore);
  }

  @Test
  public void copySnapshotToFollower() throws Exception {
    createSnapshotFile(mLeaderStore);
    Assert.assertNull(mFollowerStore.getLatestSnapshot());

    mFollowerSnapshotManager.installSnapshotFromLeader();

    CommonUtils.waitFor("follower snapshot to complete",
        () -> mFollowerStore.getLatestSnapshot() != null, mWaitOptions);
    validateSnapshotFile(mFollowerStore);
  }
}
