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
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.master.PrimarySelector;
import alluxio.master.journal.AbstractJournalSystem;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * System for multiplexing many logical journals into a single raft-based journal.
 *
 * This class embeds a CopycatServer which implements the raft algorithm, replicating entries across
 * a majority of servers before applying them to the state machine. To make the Copycat system work
 * as an Alluxio journal system, we implement two non-standard behaviors: (1) pre-applying
 * operations on the primary and (2) tightly controlling primary snapshotting.
 * <h1>Pre-apply</h1>
 * <p>
 * Unlike the Copycat framework, Alluxio updates state machine state *before* writing to the
 * journal. This lets us avoid journaling operations which do not result in state modification. To
 * make this work in the Copycat framework, we allow RPCs to modify state directly, then write an
 * entry to Copycat afterwards. Once the entry is journaled, Copycat will attempt to apply the
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
 * primary state snapshots. Normally Copycat would decide when it wants a snapshot,
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
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalSystem.class);

  /// Lifecycle: constant from when the journal system is constructed.

  private final RaftJournalConfiguration mConf;
  /**
   * Whenever in-memory state may be inconsistent with the state represented by all flushed journal
   * entries, a read lock on this lock must be held.
   * We take a write lock when we want to perform a snapshot.
   */
  private final ReadWriteLock mJournalStateLock;
  /** Controls whether Copycat will attempt to take snapshots. */
  private final AtomicBoolean mSnapshotAllowed;
  /**
   * Listens to the copycat server to detect gaining or losing primacy. The lifecycle for this
   * object is the same as the lifecycle of the {@link RaftJournalSystem}. When the copycat server
   * is reset during failover, this object must be re-initialized with the new server.
   */
  private final RaftPrimarySelector mPrimarySelector;
  /**
   * Client used for submitting empty journal entries during snapshot. This client is created once
   * and used for the lifetime of the journal system.
   */
  private final CompletableFuture<CopycatClient> mSnapshotClient;

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
  private CopycatServer mServer;

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

  /**
   * @param conf raft journal configuration
   */
  private RaftJournalSystem(RaftJournalConfiguration conf) {
    Preconditions.checkState(conf.getMaxLogSize() <= Integer.MAX_VALUE,
        "{} has value {} but must not exceed {}", PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX,
        conf.getMaxLogSize(), Integer.MAX_VALUE);
    Preconditions.checkState(conf.getHeartbeatIntervalMs() < conf.getElectionTimeoutMs() / 2,
        "Heartbeat interval (%sms) should be less than half of the election timeout (%sms)",
        conf.getHeartbeatIntervalMs(), conf.getElectionTimeoutMs());
    mConf = conf;
    mJournals = new ConcurrentHashMap<>();
    mSnapshotAllowed = new AtomicBoolean(true);
    mJournalStateLock = new ReentrantReadWriteLock(true);
    mPrimarySelector = new RaftPrimarySelector();
    mAsyncJournalWriter = new AtomicReference<>();
    mSnapshotClient = createClient().connect();
  }

  /**
   * Creates and initializes a raft journal system.
   *
   * @param conf raft journal configuration
   * @return the created raft journal system
   */
  public static RaftJournalSystem create(RaftJournalConfiguration conf) {
    RaftJournalSystem system = new RaftJournalSystem(conf);
    system.initServer();
    return system;
  }

  private synchronized void initServer() {
    LOG.debug("Creating journal with max segment size {}", mConf.getMaxLogSize());
    Storage storage =
        Storage.builder()
            .withDirectory(mConf.getPath())
            .withStorageLevel(StorageLevel.valueOf(mConf.getStorageLevel().name()))
            // Minor compaction happens anyway after snapshotting. We only free entries when
            // snapshotting, so there is no benefit to regular minor compaction cycles. We set a
            // high value because infinity is not allowed. If we set this too high it will overflow.
            .withMinorCompactionInterval(Duration.ofDays(200))
            .withMaxSegmentSize((int) mConf.getMaxLogSize())
            .build();
    if (mStateMachine != null) {
      mStateMachine.close();
    }
    mStateMachine = new JournalStateMachine(mJournals);
    mServer = CopycatServer.builder(getLocalAddress(mConf))
        .withStorage(storage)
        .withElectionTimeout(Duration.ofMillis(mConf.getElectionTimeoutMs()))
        .withHeartbeatInterval(Duration.ofMillis(mConf.getHeartbeatIntervalMs()))
        .withSnapshotAllowed(mSnapshotAllowed)
        .withSerializer(createSerializer())
        .withTransport(new NettyTransport())
        // Copycat wants a supplier that will generate *new* state machines. We can't handle
        // generating a new state machine here, so we will throw an exception if copycat tries to
        // call the supplier more than once.
        // TODO(andrew): Ensure that this supplier will really only be called once.
        .withStateMachine(new OnceSupplier<>(mStateMachine))
        .build();
    mPrimarySelector.init(mServer);
  }

  private CopycatClient createClient() {
    return CopycatClient.builder(getClusterAddresses(mConf))
        .withRecoveryStrategy(RecoveryStrategies.RECOVER)
        .withConnectionStrategy(attempt -> attempt.retry(Duration.ofMillis(
            Math.min(Math.round(100D * Math.pow(2D, (double) attempt.attempt())), 1000L))))
        .build();
  }

  private static List<Address> getClusterAddresses(RaftJournalConfiguration conf) {
    return conf.getClusterAddresses().stream()
        .map(addr -> new Address(addr.getHostName(), addr.getPort()))
        .collect(Collectors.toList());
  }

  private static Address getLocalAddress(RaftJournalConfiguration conf) {
    return new Address(conf.getLocalAddress().getHostName(), conf.getLocalAddress().getPort());
  }

  /**
   * @return the serializer for commands in the {@link StateMachine}
   */
  public static Serializer createSerializer() {
    return new Serializer().register(JournalEntryCommand.class, 1);
  }

  @Override
  public synchronized Journal createJournal(JournalEntryStateMachine master) {
    RaftJournal journal = new RaftJournal(master, mConf.getPath().toURI(), mAsyncJournalWriter,
        mJournalStateLock.readLock());
    mJournals.put(master.getName(), journal);
    return journal;
  }

  @Override
  public synchronized void gainPrimacy() {
    mSnapshotAllowed.set(false);
    CopycatClient client = createClient();
    try {
      client.connect().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_CONNECT.getMessage(
          Arrays.toString(getClusterAddresses(mConf).toArray()), e.getCause().toString());
      throw new RuntimeException(errorMessage, e.getCause());
    }
    try {
      catchUp(mStateMachine, client);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    long nextSN = mStateMachine.upgrade() + 1;

    Preconditions.checkState(mRaftJournalWriter == null);
    mRaftJournalWriter = new RaftJournalWriter(nextSN, client);
    mAsyncJournalWriter.set(new AsyncJournalWriter(mRaftJournalWriter));
  }

  @Override
  public synchronized void losePrimacy() {
    try {
      mRaftJournalWriter.close();
    } catch (IOException e) {
      LOG.warn("Error closing journal writer: {}", e.toString());
    } finally {
      mAsyncJournalWriter.set(null);
      mRaftJournalWriter = null;
    }
    LOG.info("Shutting down Raft server");
    try {
      mServer.shutdown().get();
    } catch (ExecutionException e) {
      LOG.error("Fatal error: failed to leave Raft cluster while stepping down", e);
      System.exit(-1);
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while leaving Raft cluster");
    }
    LOG.info("Shut down Raft server");
    mSnapshotAllowed.set(true);
    initServer();
    LOG.info("Bootstrapping new Raft server");
    try {
      mServer.bootstrap(getClusterAddresses(mConf)).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while rejoining Raft cluster");
    } catch (ExecutionException e) {
      LOG.error("Fatal error: failed to rejoin Raft cluster with addresses {} while stepping down",
          getClusterAddresses(mConf), e);
      System.exit(-1);
    }

    LOG.info("Raft server successfully restarted");
  }

  /**
   * Attempts to catch up. If the master loses leadership during this method, it will return early.
   *
   * The caller is responsible for detecting and responding to leadership changes.
   */
  private void catchUp(JournalStateMachine stateMachine, CopycatClient client)
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
      CompletableFuture<Void> future = client.submit(new JournalEntryCommand(
          JournalEntry.newBuilder().setSequenceNumber(gainPrimacySN).build()));
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
    LOG.info("Starting Raft journal system");
    long startTime = System.currentTimeMillis();
    try {
      mServer.bootstrap(getClusterAddresses(mConf)).get();
    } catch (ExecutionException e) {
      String errorMessage = ExceptionMessage.FAILED_RAFT_BOOTSTRAP.getMessage(
          Arrays.toString(getClusterAddresses(mConf).toArray()), e.getCause().toString());
      throw new IOException(errorMessage, e.getCause());
    }
    LOG.info("Started Raft Journal System in {}ms. Cluster addresses: {}. Local address: {}",
        System.currentTimeMillis() - startTime, getClusterAddresses(mConf), getLocalAddress(mConf));
  }

  @Override
  public synchronized void stopInternal() throws InterruptedException, IOException {
    LOG.info("Shutting down raft journal");
    mRaftJournalWriter.close();
    try {
      mServer.shutdown().get(2, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to shut down Raft server", e);
    } catch (TimeoutException e) {
      LOG.info("Timed out shutting down raft server");
    }
    LOG.info("Journal shutdown complete");
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
    FileUtils.deletePathRecursively(mConf.getPath().getAbsolutePath());
    mConf.getPath().mkdirs();
  }

  /**
   * @return a primary selector backed by leadership within the Raft cluster
   */
  public PrimarySelector getPrimarySelector() {
    return mPrimarySelector;
  }
}
