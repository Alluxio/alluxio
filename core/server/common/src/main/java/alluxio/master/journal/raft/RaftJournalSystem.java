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

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.CancelledException;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.QuorumServerState;
import alluxio.master.Master;
import alluxio.master.PrimarySelector;
import alluxio.master.journal.AbstractJournalSystem;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import io.atomix.catalyst.serializer.Serializer;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * System for multiplexing many logical journals into a single raft-based journal.
 *
 * This class embeds a RaftServer which implements the raft algorithm, replicating entries across
 * a majority of servers before applying them to the state machine. To make the Ratis system work
 * as an Alluxio journal system, we implement two non-standard behaviors: (1) pre-applying
 * operations on the primary and (2) tightly controlling primary snapshotting.
 * <h1>Pre-apply</h1>
 * <p>
 * Unlike the Ratis framework, Alluxio updates state machine state *before* writing to the
 * journal. This lets us avoid journaling operations which do not result in state modification. To
 * make this work in the Ratis framework, we allow RPCs to modify state directly, then write an
 * entry to Ratis afterwards. Once the entry is journaled, Ratis will attempt to apply the
 * journal entry to each master. The entry has already been applied on the primary, so we treat all
 * journal entries applied to the primary as no-ops to avoid double-application.
 *
 * <h2>Correctness of pre-apply</h2>
 * <p>
 * There are two cases to worry about: (1) incorrectly ignoring an entry and (2) incorrectly
 * applying an entry.
 *
 * <h3> Avoid incorrectly ignoring entries</h3>
 * <p>
 * This could happen if a server thinks it is the primary, and ignores a journal entry served from
 * the real primary. To prevent this, primaries wait for a quiet period before serving requests.
 * During this time, the previous primary will go through at least two election cycles without
 * successfully sending heartbeats to the majority of the cluster. If the old primary successfully
 * sent a heartbeat to a node which elected the new primary, the old primary would realize it isn't
 * primary and step down. Therefore, the old primary will step down and no longer ignore requests by
 * the time the new primary begins sending entries.
 *
 * <h3> Avoid incorrectly applying entries</h3>
 * <p>
 * Entries can never be double-applied to a primary's state because as long as it is the primary, it
 * will ignore all entries, and once it becomes secondary, it will completely reset its state and
 * rejoin the cluster.
 *
 * <h1>Snapshot control</h1>
 * <p>
 * The way we apply journal entries to the primary makes it tricky to perform
 * primary state snapshots. Normally Ratis would decide when it wants a snapshot,
 * but with the pre-apply protocol we may be in the middle of modifying state
 * when the snapshot would happen. To manage this, we inject an AtomicBoolean into Copycat
 * which decides whether it will be allowed to take snapshots. Normally, snapshots
 * are prohibited on the primary. However, we don't want the primary's log to grow unbounded,
 * so we allow a snapshot to be taken once a day at a user-configured time. To support this,
 * all state changes must first acquire a read lock, and snapshotting requires the
 * corresponding write lock. Once we have the write lock for all state machines, we enable
 * snapshots in Copycat through our AtomicBoolean, then wait for any snapshot to complete.
 */
@ThreadSafe
public final class RaftJournalSystem extends AbstractJournalSystem {
  public static final UUID RAFT_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalSystem.class);

  // Election timeout to use in a single master cluster.
  private static final long SINGLE_MASTER_ELECTION_TIMEOUT_MS = 500;

  /// Lifecycle: constant from when the journal system is constructed.

  private final RaftJournalConfiguration mConf;

  /** Controls whether Copycat will attempt to take snapshots. */
  private final AtomicBoolean mSnapshotAllowed;
  /**
   * Listens to the copycat server to detect gaining or losing primacy. The lifecycle for this
   * object is the same as the lifecycle of the {@link RaftJournalSystem}. When the copycat server
   * is reset during failover, this object must be re-initialized with the new server.
   */
  private final RaftPrimarySelector mPrimarySelector;

  /// Lifecycle: constant from when the journal system is started.

  /** Contains all journals created by this journal system. */
  private final ConcurrentHashMap<String, RaftJournal> mJournals;

  /// Lifecycle: created at startup and re-created when master loses primacy and resets.

  /**
   * Interacts with Copycat, applying entries to masters, taking snapshots,
   * and installing snapshots.
   */
  private JournalStateMachine mStateMachine;
  /**
   * Copycat server.
   */
  private RaftServer mServer;

  /// Lifecycle: created when gaining primacy, destroyed when losing primacy.

  /**
   * Writer which uses a copycat client to write journal entries. This field is only set when the
   * journal system is primary mode. When primacy is lost, the writer is closed and set to null.
   */
  private RaftJournalWriter mRaftJournalWriter;
  /**
   * Reference to the journal writer shared by all journals. When RPCs create journal contexts, they
   * will use the writer within this reference. The writer is null when the journal is in secondary
   * mode.
   */
  private final AtomicReference<AsyncJournalWriter> mAsyncJournalWriter;
  private final ClientId mClientId = ClientId.randomId();
  private RaftGroup mRaftGroup;
  private RaftGroupId mRaftGroupId;
  private RaftPeerId mPeerId;
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  /**
   * @param conf raft journal configuration
   */
  private RaftJournalSystem(RaftJournalConfiguration conf) {
    mConf = processRaftConfiguration(conf);
    mJournals = new ConcurrentHashMap<>();
    mSnapshotAllowed = new AtomicBoolean(true);
    mPrimarySelector = new RaftPrimarySelector();
    mAsyncJournalWriter = new AtomicReference<>();
  }

  /**
   * Creates and initializes a raft journal system.
   *
   * @param conf raft journal configuration
   * @return the created raft journal system
   */
  public static RaftJournalSystem create(RaftJournalConfiguration conf) {
    RaftJournalSystem system = new RaftJournalSystem(conf);
    return system;
  }

  private RaftJournalConfiguration processRaftConfiguration(RaftJournalConfiguration conf) {
    // Override election/heartbeat timeouts for single master cluster
    // if election timeout is not set explicitly.
    // This is to speed up single master cluster boot-up.
    if (conf.getClusterAddresses().size() == 1
        && !ServerConfiguration.isSetByUser(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT)) {
      LOG.debug("Overriding election timeout to {}ms for single master cluster.",
          SINGLE_MASTER_ELECTION_TIMEOUT_MS);
      conf.setElectionTimeoutMs(SINGLE_MASTER_ELECTION_TIMEOUT_MS);
      // Use the highest heartbeat internal relative to election timeout.
      conf.setHeartbeatIntervalMs((SINGLE_MASTER_ELECTION_TIMEOUT_MS / 2) - 1);
    }
    // Validate the conf.
    conf.validate();
    return conf;
  }

  private RaftPeerId getPeerId(InetSocketAddress address) {
    return RaftPeerId.getRaftPeerId(address.getHostString() + "_" + address.getPort());
  }

  private synchronized void initServer() throws IOException {
    LOG.debug("Creating journal with max segment size {}", mConf.getMaxLogSize());
    if (mStateMachine != null) {
      mStateMachine.close();
    }
    mStateMachine = new JournalStateMachine(mJournals, this);

    List<InetSocketAddress> addresses = mConf.getClusterAddresses();
    InetSocketAddress localAddress = mConf.getLocalAddress();
    mPeerId = getPeerId(localAddress);
    Set<RaftPeer> peers = addresses.stream()
        .map(addr -> new RaftPeer(getPeerId(addr), addr))
        .collect(Collectors.toSet());
    mRaftGroupId = RaftGroupId.valueOf(RAFT_GROUP_ID);
    mRaftGroup = RaftGroup.valueOf(mRaftGroupId, peers);

    RaftProperties properties = new RaftProperties();

    // TODO(feng): implement a custom RpcType to integrate with Alluxio authentication service
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);

    // RPC port
    GrpcConfigKeys.Server.setPort(properties, localAddress.getPort());

    // storage path
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(mConf.getPath()));

    // segment size
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(mConf.getMaxLogSize()));

    // election timeout, heartbeat timeout is automatically 1/2 of the value
    final TimeDuration leaderElectionMinTimeout = TimeDuration.valueOf(
        mConf.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        leaderElectionMinTimeout);
    long leaderElectionMaxTimeout = leaderElectionMinTimeout.toLong(
        TimeUnit.MILLISECONDS) + 1000;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));

    // request timeout
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));

    // snapshot retention
    RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, 5);

    // snapshot interval
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(
        properties, true);
    long snapshotAutoTriggerThreshold =
        ServerConfiguration.global().getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties,
        snapshotAutoTriggerThreshold);

    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(
        properties, false);

    long messageSize = ServerConfiguration.global().getBytes(
        PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE);
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(messageSize));

    // TODO(feng): clean up embedded journal configuration
    // build server
    mServer = RaftServer.newBuilder()
        .setServerId(mPeerId)
        .setGroup(mRaftGroup)
        .setStateMachine(mStateMachine)
        .setProperties(properties)
        .build();
  }

  private RaftClient createClient() {
    RaftProperties properties = new RaftProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(15, TimeUnit.SECONDS));
    RetryPolicy retryPolicy = ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
        .setMaxAttempts(10)
        .setMaxSleepTime(TimeDuration.ONE_SECOND)
        .build();
    return RaftClient.newBuilder()
        .setRaftGroup(mRaftGroup)
        .setLeaderId(null)
        .setProperties(properties)
        .setParameters(new Parameters())
        .setRetryPolicy(retryPolicy)
        .build();
  }

  /**
   * @return the serializer for commands in the StateMachine
   */
  public static Serializer createSerializer() {
    return new Serializer().register(JournalEntryCommand.class, 1);
  }

  @Override
  public synchronized Journal createJournal(Master master) {
    RaftJournal journal = new RaftJournal(master, mConf.getPath().toURI(), mAsyncJournalWriter);
    mJournals.put(master.getName(), journal);
    return journal;
  }

  @Override
  public synchronized void gainPrimacy() {
    mSnapshotAllowed.set(false);
    RaftClient client = createClient();
    Runnable closeClient = () -> {
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Failed to close raft client: {}", e.toString());
      }
    };

    try {
      catchUp(mStateMachine, client);
    } catch (TimeoutException e) {
      closeClient.run();
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      closeClient.run();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    long nextSN = mStateMachine.upgrade() + 1;

    Preconditions.checkState(mRaftJournalWriter == null);
    mRaftJournalWriter = new RaftJournalWriter(nextSN, client);
    mAsyncJournalWriter
        .set(new AsyncJournalWriter(mRaftJournalWriter, () -> this.getJournalSinks(null)));
  }

  @Override
  public synchronized void losePrimacy() {
    if (mServer.getLifeCycleState() != LifeCycle.State.RUNNING) {
      // Avoid duplicate shut down copycat server
      return;
    }
    try {
      // Close async writer first to flush pending entries.
      mAsyncJournalWriter.get().close();
      mRaftJournalWriter.close();
    } catch (IOException e) {
      LOG.warn("Error closing journal writer: {}", e.toString());
    } finally {
      mAsyncJournalWriter.set(null);
      mRaftJournalWriter = null;
    }
    LOG.info("Shutting down Raft server");
    try {
      mServer.close();
    } catch (IOException e) {
      LOG.error("Fatal error: failed to leave Raft cluster while stepping down", e);
      System.exit(-1);
      throw new IllegalStateException(e);
    }
    LOG.info("Shut down Raft server");
    try {
      initServer();
    } catch (IOException e) {
      LOG.error("Fatal error: failed to init Raft cluster with addresses {} while stepping down",
          mConf.getClusterAddresses(), e);
      System.exit(-1);
    }
    LOG.info("Bootstrapping new Raft server");
    try {
      mServer.start();
    } catch (IOException e) {
      LOG.error("Fatal error: failed to start Raft cluster with addresses {} while stepping down",
          mConf.getClusterAddresses(), e);
      System.exit(-1);
    }

    LOG.info("Raft server successfully restarted");
  }

  @Override
  public synchronized Map<String, Long> getCurrentSequenceNumbers() {
    long currentGlobalState = mStateMachine.getLastAppliedSequenceNumber();
    Map<String, Long> sequenceMap = new HashMap<>();
    for (String master : mJournals.keySet()) {
      // Return the same global sequence for each master.
      sequenceMap.put(master, currentGlobalState);
    }
    return sequenceMap;
  }

  @Override
  public synchronized void suspend() throws IOException {
    mStateMachine.suspend();
  }

  @Override
  public synchronized void resume() throws IOException {
    mStateMachine.resume();
  }

  @Override
  public synchronized CatchupFuture catchup(Map<String, Long> journalSequenceNumbers) {
    // Given sequences should be the same for each master for embedded journal.
    List<Long> distinctSequences =
        journalSequenceNumbers.values().stream().distinct().collect(Collectors.toList());
    Preconditions.checkState(distinctSequences.size() == 1, "incorrect journal sequences");
    return mStateMachine.catchup(distinctSequences.get(0));
  }

  static Message toRaftMessage(JournalEntry entry) {
    return Message.valueOf(UnsafeByteOperations.unsafeWrap(
        new JournalEntryCommand(entry).getSerializedJournalEntry()));
  }

  @Override
  public synchronized void checkpoint() throws IOException {
    // TODO(feng): consider removing this once we can automatically propagate
    //             snapshots from secondary master
    try (RaftClient client = createClient()) {
      catchUp(mStateMachine, client);
      mStateMachine.takeLocalSnapshot();
      // TODO(feng): maybe prune logs after snapshot
    } catch (TimeoutException e) {
      LOG.warn("Timeout while performing snapshot: {}", e.toString());
      throw new IOException("Timeout while performing snapshot", e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while performing snapshot: {}", e.toString());
      Thread.currentThread().interrupt();
      throw new CancelledException("Interrupted while performing snapshot", e);
    }
  }

  /**
   * Attempts to catch up. If the master loses leadership during this method, it will return early.
   *
   * The caller is responsible for detecting and responding to leadership changes.
   */
  private void catchUp(JournalStateMachine stateMachine, RaftClient client)
      throws TimeoutException, InterruptedException {
    long startTime = System.currentTimeMillis();
    // Wait for any outstanding snapshot to complete.
    CommonUtils.waitFor("snapshotting to finish", () -> !stateMachine.isSnapshotting(),
        WaitForOptions.defaults().setTimeoutMs(10 * Constants.MINUTE_MS));

    // Loop until we lose leadership or convince ourselves that we are caught up and we are the only
    // master serving. To convince ourselves of this, we need to accomplish three steps:
    //
    // 1. Write a unique ID to the copycat.
    // 2. Wait for the ID to by applied to the state machine. This proves that we are
    //    caught up since the copycat cannot apply commits from a previous term after applying
    //    commits from a later term.
    // 3. Wait for a quiet period to elapse without anything new being written to Copycat. This is a
    //    heuristic to account for the time it takes for a node to realize it is no longer the
    //    leader. If two nodes think they are leader at the same time, they will both write unique
    //    IDs to the journal, but only the second one has a chance of becoming leader. The first
    //    will see that an entry was written after its ID, and double check that it is still the
    //    leader before trying again.
    while (true) {
      if (mPrimarySelector.getState() != PrimarySelector.State.PRIMARY) {
        return;
      }
      long lastAppliedSN = stateMachine.getLastAppliedSequenceNumber();
      long gainPrimacySN = ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, 0);
      LOG.info("Performing catchup. Last applied SN: {}. Catchup ID: {}",
          lastAppliedSN, gainPrimacySN);
      CompletableFuture<RaftClientReply> future = client.sendAsync(
          toRaftMessage(JournalEntry.newBuilder().setSequenceNumber(gainPrimacySN).build()));
      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (TimeoutException | ExecutionException e) {
        LOG.info("Exception submitting term start entry: {}", e.toString());
        continue;
      }

      try {
        CommonUtils.waitFor("term start entry " + gainPrimacySN
            + " to be applied to state machine", () ->
            stateMachine.getLastPrimaryStartSequenceNumber() == gainPrimacySN,
            WaitForOptions.defaults()
                .setInterval(Constants.SECOND_MS)
                .setTimeoutMs(5 * Constants.SECOND_MS));
      } catch (TimeoutException e) {
        LOG.info(e.toString());
        continue;
      }

      // Wait 2 election timeouts so that this master and other masters have time to realize they
      // are not leader.
      CommonUtils.sleepMs(2 * mConf.getElectionTimeoutMs());
      if (stateMachine.getLastAppliedSequenceNumber() != lastAppliedSN
          || stateMachine.getLastPrimaryStartSequenceNumber() != gainPrimacySN) {
        // Someone has committed a journal entry since we started trying to catch up.
        // Restart the catchup process.
        continue;
      }
      LOG.info("Caught up in {}ms. Last sequence number from previous term: {}.",
          System.currentTimeMillis() - startTime, stateMachine.getLastAppliedSequenceNumber());
      return;
    }
  }

  @Override
  public synchronized void startInternal() throws InterruptedException, IOException {
    LOG.info("Initializing Raft Journal System");
    initServer();
    List<InetSocketAddress> clusterAddresses = mConf.getClusterAddresses();
    LOG.info("Starting Raft journal system. Cluster addresses: {}. Local address: {}",
        clusterAddresses, mConf.getLocalAddress());
    long startTime = System.currentTimeMillis();
    try {
      mServer.start();
    } catch (IOException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_BOOTSTRAP
          .getMessage(Arrays.toString(clusterAddresses.toArray()), e.getCause().toString());
      throw new IOException(errorMessage, e.getCause());
    }
    LOG.info("Started Raft Journal System in {}ms", System.currentTimeMillis() - startTime);
  }

  @Override
  public synchronized void stopInternal() throws InterruptedException, IOException {
    LOG.info("Shutting down raft journal");
    if (mRaftJournalWriter != null) {
      mRaftJournalWriter.close();
    }
    try {
      mServer.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to shut down Raft server", e);
    }
    LOG.info("Journal shutdown complete");
  }

  /**
   * Used to get information of internal RAFT quorum.
   *
   * @return list of information for participating servers in RAFT quorum
   */
  public synchronized List<QuorumServerInfo> getQuorumServerInfoList() {
    List<QuorumServerInfo> quorumMemberStateList = new LinkedList<>();
    RaftProtos.LeaderInfoProto leaderInfo = getRoleInfo().getLeaderInfo();
    if (leaderInfo == null) {
      return Collections.emptyList();
    }
    for (RaftProtos.ServerRpcProto member : leaderInfo.getFollowerInfoList()) {
      HostAndPort hp = HostAndPort.fromString(member.getId().getAddress());
      NetAddress memberAddress = NetAddress.newBuilder().setHost(hp.getHost())
          .setRpcPort(hp.getPort()).build();

      quorumMemberStateList.add(QuorumServerInfo.newBuilder().setServerAddress(memberAddress)
          .setServerState(member.getLastRpcElapsedTimeMs() > mConf.getElectionTimeoutMs()
              ? QuorumServerState.UNAVAILABLE : QuorumServerState.AVAILABLE).build());
    }
    InetSocketAddress localAddress = mConf.getLocalAddress();
    NetAddress self = NetAddress.newBuilder()
        .setHost(localAddress.getHostString())
        .setRpcPort(localAddress.getPort())
        .build();
    quorumMemberStateList.add(QuorumServerInfo.newBuilder()
        .setServerAddress(self)
        .setServerState(QuorumServerState.AVAILABLE).build());
    return quorumMemberStateList;
  }

  private GroupInfoReply getGroupInfo() throws IOException {
    GroupInfoRequest groupInfoRequest = new GroupInfoRequest(mClientId,
        mPeerId, mRaftGroupId, nextCallId());
    return mServer.getGroupInfo(groupInfoRequest);
  }

  private RaftProtos.RoleInfoProto getRoleInfo() {
    GroupInfoReply groupInfo = null;
    try {
      groupInfo = getGroupInfo();
    } catch (IOException e) {
      LOG.warn("failed to get leadership info", e);
      return null;
    }
    return groupInfo.getRoleInfoProto();
  }

  /**
   * @return {@code true} if this journal system is the leader
   */
  @VisibleForTesting
  public synchronized boolean isLeader() {
    return mServer != null
        && mServer.getLifeCycleState() == LifeCycle.State.RUNNING
        && mPrimarySelector.getState() == PrimarySelector.State.PRIMARY;
  }

  /**
   * Removes from RAFT quorum, a server with given address.
   * For server to be removed, it should be in unavailable state in quorum.
   *
   * @param serverNetAddress address of the server to remove from the quorum
   * @throws IOException
   */
  public synchronized void removeQuorumServer(NetAddress serverNetAddress) throws IOException {
    InetSocketAddress serverAddress = InetSocketAddress
        .createUnresolved(serverNetAddress.getHost(), serverNetAddress.getRpcPort());
    RaftPeerId peerId = getPeerId(serverAddress);
    try (RaftClient client = createClient()) {
      Collection<RaftPeer> peers = mServer.getGroups().iterator().next().getPeers();
      RaftClientReply reply = client.setConfiguration(peers.stream()
          .filter(peer -> !peer.getId().equals(peerId))
          .toArray(RaftPeer[]::new));
      if (reply.getException() != null) {
        throw reply.getException();
      }
    }
  }

  @Override
  public synchronized boolean isEmpty() {
    return mRaftJournalWriter != null && mRaftJournalWriter.getNextSequenceNumberToWrite() == 0;
  }

  @Override
  public boolean isFormatted() {
    return mConf.getPath().exists();
  }

  @Override
  public void format() throws IOException {
    File journalPath = mConf.getPath();
    if (journalPath.isDirectory()) {
      org.apache.commons.io.FileUtils.cleanDirectory(mConf.getPath());
    } else {
      if (journalPath.exists()) {
        FileUtils.delete(journalPath.getAbsolutePath());
      }
      journalPath.mkdirs();
    }
  }

  /**
   * @return a primary selector backed by leadership within the Raft cluster
   */
  public PrimarySelector getPrimarySelector() {
    return mPrimarySelector;
  }

  /**
   * Notifies the journal that the leadership state has changed.
   * @param isLeader whether the local server is teh current leader
   */
  public void notifyLeadershipStateChanged(boolean isLeader) {
    mPrimarySelector.notifyStateChanged(
        isLeader ? PrimarySelector.State.PRIMARY : PrimarySelector.State.SECONDARY);
  }
}
