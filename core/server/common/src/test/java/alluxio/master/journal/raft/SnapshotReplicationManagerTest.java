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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.JournalQueryRequest;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.RaftJournalServiceGrpc;
import alluxio.grpc.SnapshotData;
import alluxio.grpc.UploadSnapshotPRequest;
import alluxio.grpc.UploadSnapshotPResponse;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import net.bytebuddy.utility.RandomString;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SnapshotReplicationManagerTest {
  private static final int SNAPSHOT_SIZE = 100_000;
  private static final int DEFAULT_SNAPSHOT_TERM = 0;
  private static final int DEFAULT_SNAPSHOT_INDEX = 1;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_CHUNK_SIZE,
          "32KB", Configuration.modifiableGlobal());

  private final WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(30_000);
  private SnapshotReplicationManager mLeaderSnapshotManager;
  private RaftJournalSystem mLeader;
  private SimpleStateMachineStorage mLeaderStore;
  private final Map<RaftPeerId, Follower> mFollowers = new HashMap<>();

  private RaftJournalServiceClient mClient;
  private Server mServer;

  private void before(int numFollowers) throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_REQUEST_INFO_TIMEOUT, 550);
    Configuration.set(PropertyKey.MASTER_JOURNAL_REQUEST_DATA_TIMEOUT, 550);
    mLeader = Mockito.mock(RaftJournalSystem.class);
    Mockito.when(mLeader.isLeader()).thenReturn(true);
    Mockito.when(mLeader.getLocalPeerId()).thenReturn(RaftPeerId.getRaftPeerId("leader"));
    mLeaderStore = getSimpleStateMachineStorage();
    mLeaderSnapshotManager = Mockito.spy(new SnapshotReplicationManager(mLeader, mLeaderStore));

    String serverName = InProcessServerBuilder.generateName();
    mServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new RaftJournalServiceHandler(mLeaderSnapshotManager)).build();
    mServer.start();
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    RaftJournalServiceGrpc.RaftJournalServiceStub stub = RaftJournalServiceGrpc.newStub(channel);
    // mock RaftJournalServiceClient
    mClient = Mockito.mock(RaftJournalServiceClient.class);
    Mockito.doNothing().when(mClient).close();
    // download rpc mock
    Mockito.when(mClient.downloadSnapshot(any())).thenAnswer((args) -> {
      StreamObserver responseObserver = args.getArgument(0, StreamObserver.class);
      return stub.downloadSnapshot(responseObserver);
    });
    // upload rpc mock
    Mockito.when(mClient.uploadSnapshot(any())).thenAnswer((args) -> {
      StreamObserver responseObserver = args.getArgument(0, StreamObserver.class);
      return stub.uploadSnapshot(responseObserver);
    });
    Mockito.doReturn(mClient).when(mLeaderSnapshotManager).createJournalServiceClient();

    for (int i = 0; i < numFollowers; i++) {
      Follower follower = new Follower(mClient);
      mFollowers.put(follower.getRaftPeerId(), follower);
    }

    List<QuorumServerInfo> quorumServerInfos = mFollowers.values().stream().map(follower -> {
      return QuorumServerInfo.newBuilder().setServerAddress(
          NetAddress.newBuilder().setHost(follower.mHost).setRpcPort(follower.mRpcPort)).build();
    }).collect(Collectors.toList());

    Mockito.when(mLeader.getQuorumServerInfoList()).thenReturn(quorumServerInfos);
    Answer<?> fn = (args) -> {
      RaftPeerId peerId = args.getArgument(0, RaftPeerId.class);
      Message message = args.getArgument(1, Message.class);
      JournalQueryRequest queryRequest = JournalQueryRequest.parseFrom(
          message.getContent().asReadOnlyByteBuffer());
      return CompletableFuture.supplyAsync(() -> {
        CompletableFuture<RaftClientReply> fut = CompletableFuture.supplyAsync(() -> {
          Message response;
          try {
            response = mFollowers.get(peerId).mSnapshotManager.handleRequest(queryRequest);
          } catch (IOException e) {
            throw new CompletionException(e);
          }
          RaftClientReply reply = Mockito.mock(RaftClientReply.class);
          Mockito.when(reply.getMessage()).thenReturn(response);
          return reply;
        });
        RaftClientReply result;
        try {
          if (args.getArguments().length == 3) {
            result = fut.get(args.getArgument(2), TimeUnit.MILLISECONDS);
          } else {
            result = fut.get();
          }
          return result;
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      });
    };
    Mockito.when(mLeader.sendMessageAsync(any(), any())).thenAnswer(fn);
    Mockito.when(mLeader.sendMessageAsync(any(), any(), anyLong())).thenAnswer(fn);
  }

  private SimpleStateMachineStorage getSimpleStateMachineStorage() throws IOException {
    RaftStorage rs = new RaftStorageImpl(mFolder.newFolder(CommonUtils.randomAlphaNumString(6)),
        RaftServerConfigKeys.Log.CorruptionPolicy.getDefault(),
        RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize());
    SimpleStateMachineStorage snapshotStore = new SimpleStateMachineStorage();
    snapshotStore.init(rs);
    return snapshotStore;
  }

  private void createSnapshotFile(SimpleStateMachineStorage storage) throws IOException {
    createSnapshotFile(storage, DEFAULT_SNAPSHOT_TERM, DEFAULT_SNAPSHOT_INDEX);
  }

  private void createSnapshotFile(SimpleStateMachineStorage storage, long term, long index)
      throws IOException {
    java.io.File file = storage.getSnapshotFile(term, index);
    FileUtils.writeByteArrayToFile(file, BufferUtils.getIncreasingByteArray(SNAPSHOT_SIZE));
    storage.loadLatestSnapshot();
  }

  private void validateSnapshotFile(SimpleStateMachineStorage storage) throws IOException {
    validateSnapshotFile(storage, DEFAULT_SNAPSHOT_TERM, DEFAULT_SNAPSHOT_INDEX);
  }

  private void validateSnapshotFile(SimpleStateMachineStorage storage, long term, long index)
      throws IOException {
    SingleFileSnapshotInfo snapshot = storage.getLatestSnapshot();
    Assert.assertNotNull(snapshot);
    Assert.assertEquals(TermIndex.valueOf(term, index), snapshot.getTermIndex());
    byte[] received = FileUtils.readFileToByteArray(snapshot.getFiles().get(0).getPath().toFile());
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SNAPSHOT_SIZE, received));
  }

  @After
  public void After() throws Exception {
    mServer.shutdown();
    mServer.awaitTermination();
  }

  @Test
  public void copySnapshotToLeader() throws Exception {
    before(1);
    Follower follower = mFollowers.values().stream().findFirst().get();
    createSnapshotFile(follower.mStore);

    Assert.assertNull(mLeaderStore.getLatestSnapshot());
    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    validateSnapshotFile(mLeaderStore);
  }

  @Test
  public void copySnapshotToFollower() throws Exception {
    before(1);
    createSnapshotFile(mLeaderStore);

    Follower follower = mFollowers.values().stream().findFirst().get();
    Assert.assertNull(follower.mStore.getLatestSnapshot());

    follower.mSnapshotManager.installSnapshotFromLeader();

    CommonUtils.waitFor("follower snapshot to complete",
        () -> follower.mStore.getLatestSnapshot() != null, mWaitOptions);
    validateSnapshotFile(follower.mStore);
  }

  @Test
  public void requestSnapshotEqualTermHigherIndex() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore); // create default 0, 1 snapshot
    createSnapshotFile(secondFollower.mStore, 0, 2); // preferable to the default 0, 1 snapshot

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    // verify that the leader still requests and gets the best snapshot
    validateSnapshotFile(mLeaderStore, 0, 2);
  }

  @Test
  public void failGetInfoEqualTermHigherIndex() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore); // create default 0, 1 snapshot
    createSnapshotFile(secondFollower.mStore, 0, 2); // preferable to the default 0, 1 snapshot
    // the second follower will not reply to the getInfo request, so the leader will request from
    // the first after a timeout
    secondFollower.disableGetInfo();

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    // verify that the leader still requests and get the snapshot from the first follower
    validateSnapshotFile(mLeaderStore, 0, 1);
  }

  @Test
  public void failSnapshotRequestEqualTermHigherIndex() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore); // create default 0, 1 snapshot
    createSnapshotFile(secondFollower.mStore, 0, 2); // preferable to the default 0, 1 snapshot
    // the second follower will not start the snapshot upload, so the leader will request from the
    // first after a timeout
    secondFollower.disableFollowerUpload();

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    // verify that the leader still requests and get the snapshot from the first follower
    validateSnapshotFile(mLeaderStore, 0, 1);
  }

  @Test
  public void failFailThenSuccess() throws Exception {
    before(3);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore, 0, 1);
    createSnapshotFile(secondFollower.mStore, 0, 1);

    firstFollower.disableFollowerUpload();
    secondFollower.disableGetInfo();

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    try {
      CommonUtils.waitForResult("upload failure",
          () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower(),
          (num) -> num == 1,
          WaitForOptions.defaults().setInterval(10).setTimeoutMs(100));
    } catch (Exception e) {
      // expected to fail: no snapshot could be uploaded
    }

    Follower thirdFollower = followers.get(2);
    createSnapshotFile(thirdFollower.mStore, 0, 2);
    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();
    CommonUtils.waitForResult("upload failure",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower(),
        (num) -> num == 2, mWaitOptions);
    validateSnapshotFile(mLeaderStore, 0, 2);
  }

  @Test
  public void requestSnapshotHigherTermLowerIndex() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore, 1, 10);
    createSnapshotFile(secondFollower.mStore, 2, 1);

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    // verify that the leader still requests and gets the best snapshot
    validateSnapshotFile(mLeaderStore, 2, 1);
  }

  @Test
  public void installSnapshotsInSuccession() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore); // create default 0, 1 snapshot

    for (int i = 2; i < 12; i++) {
      if (i % 2 == 0) {
        createSnapshotFile(secondFollower.mStore, 0, i);
        secondFollower.notifySnapshotInstalled();
      } else {
        createSnapshotFile(firstFollower.mStore, 0, i);
        firstFollower.notifySnapshotInstalled();
      }
      CommonUtils.waitFor("leader snapshot to complete",
          () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
      validateSnapshotFile(mLeaderStore, 0, i);
    }
  }

  /**
   * Simulates a {@link SnapshotDownloader} error.
   */
  @Test
  public void downloadFailure() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore); // create default 0, 1 snapshot
    createSnapshotFile(secondFollower.mStore, 0, 2); // preferable to the default 0, 1 snapshot

    // make sure to error out when requesting the better snapshot from secondFollower
    Mockito.doAnswer(mock -> {
      SingleFileSnapshotInfo snapshot = secondFollower.mStore.getLatestSnapshot();
      StreamObserver<UploadSnapshotPResponse> responseObserver =
          SnapshotUploader.forFollower(secondFollower.mStore, snapshot);
      StreamObserver<UploadSnapshotPRequest> requestObserver = mClient
          .uploadSnapshot(responseObserver);
      requestObserver.onError(new IOException("failed snapshot upload"));
      return null;
    }).when(secondFollower.mSnapshotManager).sendSnapshotToLeader();

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    // verify that the leader still requests and gets second best snapshot
    validateSnapshotFile(mLeaderStore);
  }

  /**
   * Simulates a {@link SnapshotUploader} error.
   */
  @Test
  public void uploadFailure() throws Exception {
    before(2);
    List<Follower> followers = new ArrayList<>(mFollowers.values());
    Follower firstFollower = followers.get(0);
    Follower secondFollower = followers.get(1);

    createSnapshotFile(firstFollower.mStore); // create default 0, 1 snapshot
    createSnapshotFile(secondFollower.mStore, 0, 2); // preferable to the default 0, 1 snapshot

    // make sure to error out when requesting the better snapshot from secondFollower
    Mockito.doAnswer(mock -> {
      SingleFileSnapshotInfo snapshot = secondFollower.mStore.getLatestSnapshot();
      StreamObserver<UploadSnapshotPResponse> responseObserver =
          SnapshotUploader.forFollower(secondFollower.mStore, snapshot);
      StreamObserver<UploadSnapshotPRequest> requestObserver = mClient
          .uploadSnapshot(responseObserver);
      responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
      requestObserver.onNext(UploadSnapshotPRequest.newBuilder()
          .setData(SnapshotData.newBuilder()
              .setSnapshotTerm(snapshot.getTerm())
              .setSnapshotIndex(snapshot.getIndex())
              .setOffset(0))
          .build());
      return null;
    }).when(secondFollower.mSnapshotManager).sendSnapshotToLeader();

    mLeaderSnapshotManager.maybeCopySnapshotFromFollower();

    CommonUtils.waitFor("leader snapshot to complete",
        () -> mLeaderSnapshotManager.maybeCopySnapshotFromFollower() != -1, mWaitOptions);
    // verify that the leader still requests and gets second best snapshot
    validateSnapshotFile(mLeaderStore);
  }

  private class Follower {
    final String mHost;
    final int mRpcPort;
    final SnapshotReplicationManager mSnapshotManager;
    RaftJournalSystem mJournalSystem;
    SimpleStateMachineStorage mStore;

    Follower(RaftJournalServiceClient client) throws IOException {
      mHost = String.format("follower-%s", RandomString.make());
      mRpcPort = ThreadLocalRandom.current().nextInt(10_000, 99_999);
      mStore = getSimpleStateMachineStorage();
      mJournalSystem = Mockito.mock(RaftJournalSystem.class);
      mSnapshotManager = Mockito.spy(new SnapshotReplicationManager(mJournalSystem, mStore));
      Mockito.doReturn(client).when(mSnapshotManager).createJournalServiceClient();
    }

    void notifySnapshotInstalled() {
      synchronized (mSnapshotManager) {
        mSnapshotManager.notifyAll();
      }
    }

    void disableFollowerUpload() throws IOException {
      Mockito.doNothing().when(mSnapshotManager).sendSnapshotToLeader();
    }

    void disableGetInfo() throws IOException {
      Mockito.doAnswer((args) -> {
        synchronized (mSnapshotManager) {
          // we sleep so nothing is returned
          mSnapshotManager.wait(Configuration.global().getMs(
              PropertyKey.MASTER_JOURNAL_REQUEST_INFO_TIMEOUT));
        }
        throw new IOException("get info disabled");
      }).when(mSnapshotManager)
          .handleRequest(argThat(JournalQueryRequest::hasSnapshotInfoRequest));
    }

    RaftPeerId getRaftPeerId() {
      return RaftPeerId.valueOf(String.format("%s_%d", mHost, mRpcPort));
    }
  }
}
