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
import alluxio.ProcessUtils;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AddQuorumServerRequest;
import alluxio.grpc.JournalQueryRequest;
import alluxio.master.StateLockManager;
import alluxio.master.StateLockOptions;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.SingleEntryJournaled;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;
import alluxio.util.StreamUtils;
import alluxio.util.logging.SamplingLogger;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.LifeCycle;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A state machine for managing all of Alluxio's journaled state. Entries applied to this state
 * machine will be forwarded to the appropriate internal master.
 *
 * The state machine starts by resetting all state, then applying the entries offered by Ratis.
 * When the master becomes primary, it should wait until the state machine is up to date and no
 * other primary master is serving, then call {@link #upgrade}. Once the state machine is upgraded,
 * it will ignore all entries appended by Ratis because those entries are applied to primary
 * master state before being written to Rati.
 *
 */
@ThreadSafe
public class JournalStateMachine extends BaseStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(JournalStateMachine.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.SECOND_MS);

  private static final CompletableFuture<Message> EMPTY_FUTURE =
      CompletableFuture.completedFuture(Message.EMPTY);

  /** Journals managed by this applier. */
  private final Map<String, RaftJournal> mJournals;
  private final RaftJournalSystem mJournalSystem;
  private final RaftSnapshotManager mSnapshotManager;
  private final SnapshotDirStateMachineStorage mStorage;
  private final AtomicReference<StateLockManager> mStateLockManagerRef
      = new AtomicReference<>(null);
  @GuardedBy("this")
  private boolean mIgnoreApplys = false;
  @GuardedBy("this")
  private boolean mClosed = false;

  private volatile long mLastAppliedCommitIndex = -1;
  // The last special "primary start" sequence number applied to this state machine. These special
  // sequence numbers are identified by being negative.
  private volatile long mLastPrimaryStartSequenceNumber = 0;
  private volatile long mNextSequenceNumberToRead = 0;
  private volatile boolean mSnapshotting = false;
  private volatile boolean mIsLeader = false;

  private final ExecutorService mJournalPool = Executors.newCachedThreadPool();

  /**
   * This callback is used for interrupting someone who suspends the journal applier to work on
   * the states. It helps prevent dirty read/write of the states when the journal is reloading.
   *
   * Here is an example of interrupting backup tasks when the state machine reloads:
   *
   * - Backup worker suspends state machine before backup, passing in the callback.
   * - Backup worker writes journal entries to UFS.
   * - Raft state machine downloads a new snapshot from leader.
   * - Raft state machine transitions to PAUSE state and invokes the callback.
   * - Backup worker handles the callback and interrupts the backup tasks.
   * - Raft state machine starts reloading the states.
   * - Raft state machine finished the reload and transitions back to RUNNING state.
   */
  private volatile Runnable mInterruptCallback;

  // The last index of the latest journal snapshot
  // created by this master or downloaded from other masters
  private volatile long mSnapshotLastIndex = -1;
  private long mLastSnapshotTime = -1;
  @SuppressFBWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "Written in synchronized block, read by metrics")
  private long mLastSnapshotDurationMs = -1;
  @SuppressFBWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "Written in synchronized block, read by metrics")
  private long mLastSnapshotEntriesCount = -1;
  private long mLastSnapshotReplayDurationMs = -1;
  private long mLastSnapshotReplayEntriesCount = -1;
  /** Used to control applying to masters. */
  private BufferedJournalApplier mJournalApplier;

  /**
   * @param journals      master journals; these journals are still owned by the caller, not by the
   *                      journal state machine
   * @param journalSystem the raft journal system
   * @param storage the {@link SnapshotDirStateMachineStorage} that this state machine will use
   */
  public JournalStateMachine(Map<String, RaftJournal> journals, RaftJournalSystem journalSystem,
                             SnapshotDirStateMachineStorage storage) {
    mJournals = journals;
    mJournalApplier = new BufferedJournalApplier(journals,
        () -> journalSystem.getJournalSinks(null));
    resetState();
    LOG.info("Initialized new journal state machine");
    mJournalSystem = journalSystem;
    mStorage = storage;
    mSnapshotManager = new RaftSnapshotManager(mStorage, mJournalPool);

    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_LAST_INDEX.getName(),
        () -> mSnapshotLastIndex);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_JOURNAL_ENTRIES_SINCE_CHECKPOINT.getName(),
        () -> getLastAppliedTermIndex().getIndex() - mSnapshotLastIndex);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_JOURNAL_LAST_CHECKPOINT_TIME.getName(),
        () -> mLastSnapshotTime);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_JOURNAL_LAST_APPLIED_COMMIT_INDEX.getName(),
        () -> mLastAppliedCommitIndex);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_JOURNAL_CHECKPOINT_WARN.getName(),
        () -> getLastAppliedTermIndex().getIndex() - mSnapshotLastIndex
                > Configuration.getInt(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES)
                && System.currentTimeMillis() - mLastSnapshotTime > Configuration.getMs(
                PropertyKey.MASTER_WEB_JOURNAL_CHECKPOINT_WARNING_THRESHOLD_TIME)
    );
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_DURATION_MS.getName(),
        () -> mLastSnapshotDurationMs);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_ENTRIES_COUNT.getName(),
        () -> mLastSnapshotEntriesCount);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_REPLAY_DURATION_MS.getName(),
        () -> mLastSnapshotReplayDurationMs);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EMBEDDED_JOURNAL_LAST_SNAPSHOT_REPLAY_ENTRIES_COUNT.getName(),
        () -> mLastSnapshotReplayEntriesCount);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(server, groupId, raftStorage);
      mStorage.init(raftStorage);
      loadSnapshot(getLatestSnapshot());
    });
  }

  @Override
  public void reinitialize() throws IOException {
    LOG.info("Reinitializing state machine.");
    mStorage.loadLatestSnapshot();
    loadSnapshot(getLatestSnapshot());
    unpause();
  }

  private synchronized void loadSnapshot(SnapshotInfo snapshot) throws IOException {
    if (snapshot == null) {
      LOG.info("No snapshot to load");
      return;
    }
    try {
      resetState();
      setLastAppliedTermIndex(snapshot.getTermIndex());
      LOG.debug("Loading snapshot {}", snapshot);
      install(snapshot);
      LOG.debug("Finished loading snapshot {}", snapshot);
      mSnapshotLastIndex = getLatestSnapshot() != null ? getLatestSnapshot().getIndex() : -1;
    } catch (Exception e) {
      throw new IOException(String.format("Failed to load snapshot %s", snapshot), e);
    }
  }

  /**
   * Allows leader to take snapshots. This is used exclusively for the
   * `bin/alluxio fsadmin journal checkpoint` command.
   * @param manager allows the state machine to take a snapshot as leader by using it
   */
  void allowLeaderSnapshots(StateLockManager manager) {
    mStateLockManagerRef.set(manager);
  }

  void disallowLeaderSnapshots() {
    mStateLockManagerRef.set(null);
  }

  @Override
  public long takeSnapshot() {
    long index;
    StateLockManager stateLockManager = mStateLockManagerRef.get();
    if (!mIsLeader) {
      LOG.info("Taking local snapshot as follower");
      index = takeLocalSnapshot(false);
    } else if (stateLockManager != null) {
      // the leader has been allowed to take a local snapshot by being given a non-null
      // StateLockManager through the #allowLeaderSnapshots method
      try (LockResource stateLock = stateLockManager.lockExclusive(StateLockOptions.defaults())) {
        LOG.info("Taking local snapshot as leader");
        index = takeLocalSnapshot(true);
      } catch (Exception e) {
        return RaftLog.INVALID_LOG_INDEX;
      }
    } else {
      index = mSnapshotManager.downloadSnapshotFromOtherMasters();
    }
    // update metrics if took a snapshot
    if (index != RaftLog.INVALID_LOG_INDEX) {
      mSnapshotLastIndex = index;
      mLastSnapshotTime = System.currentTimeMillis();
      LOG.info("Took snapshot up to index {} at time {}", mSnapshotLastIndex, DateTime.now());
    }
    return index;
  }

  /**
   * @return the latest snapshot information, or null of no snapshot exists
   */
  @Override
  public SnapshotInfo getLatestSnapshot() {
    return mStorage.getLatestSnapshot();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return mStorage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    try {
      JournalQueryRequest queryRequest = JournalQueryRequest.parseFrom(
          request.getContent().asReadOnlyByteBuffer());
      LOG.debug("Received query request: {}", queryRequest);
      if (queryRequest.hasAddQuorumServerRequest()) {
        AddQuorumServerRequest addRequest = queryRequest.getAddQuorumServerRequest();
        return CompletableFuture.supplyAsync(() -> {
          try {
            mJournalSystem.addQuorumServer(addRequest.getServerAddress());
          } catch (IOException e) {
            throw new CompletionException(e);
          }
          return Message.EMPTY;
        });
      } else {
        return super.query(request);
      }
    } catch (Exception e) {
      LOG.error("failed processing request {}", request, e);
      future.completeExceptionally(e);
      return future;
    }
  }

  @Override
  public void close() {
    mClosed = true;
    MetricsSystem.removeMetrics(MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_LAST_INDEX.getName());
    MetricsSystem.removeMetrics(MetricKey.MASTER_JOURNAL_ENTRIES_SINCE_CHECKPOINT.getName());
    MetricsSystem.removeMetrics(MetricKey.MASTER_JOURNAL_LAST_CHECKPOINT_TIME.getName());
    MetricsSystem.removeMetrics(MetricKey.MASTER_JOURNAL_LAST_APPLIED_COMMIT_INDEX.getName());
    MetricsSystem.removeMetrics(MetricKey.MASTER_JOURNAL_CHECKPOINT_WARN.getName());
    mSnapshotManager.close();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      applyJournalEntryCommand(trx);
      RaftProtos.LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      // explicitly return empty future since no response message is expected by the journal writer
      // avoid using super.applyTransaction() since it will echo the message and add overhead
      return EMPTY_FUTURE;
    } catch (Exception e) {
      return RaftJournalUtils.completeExceptionally(e);
    }
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    mIsLeader = false;
    mJournalSystem.notifyLeadershipStateChanged(false);
  }

  @Override
  public void notifyConfigurationChanged(long term, long index,
      RaftProtos.RaftConfigurationProto newRaftConfiguration) {
    CompletableFuture.runAsync(mJournalSystem::updateGroup, mJournalPool);
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    // this method is called automatically by Ratis when the leader does not have all the logs to
    // give to this follower. This method instructs the follower to download a snapshot from
    // other masters to become up-to-date.
    LOG.info("Received instruction to install snapshot from other master asynchronously");
    return CompletableFuture.supplyAsync(() -> {
      mSnapshotManager.downloadSnapshotFromOtherMasters();
      long index = mSnapshotManager.waitForAttemptToComplete();
      if (index == RaftLog.INVALID_LOG_INDEX) {
        LOG.info("Failed to install snapshot from other master asynchronously");
        return null;
      }
      return getLatestSnapshot().getTermIndex();
    }, mJournalPool);
  }

  @Override
  public synchronized void pause() {
    LOG.info("Pausing raft state machine.");
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    if (mInterruptCallback != null) {
      LOG.info("Invoking suspension interrupt callback.");
      mInterruptCallback.run();
      mInterruptCallback = null;
    }
    try {
      if (mJournalApplier.isSuspended()) {
        // make sure there are no pending entries
        LOG.info("Resuming journal applier.");
        mJournalApplier.resume();
      }
    } catch (IOException e) {
      throw new IllegalStateException("State machine pause failed", e);
    }
    getLifeCycle().transition(LifeCycle.State.PAUSED);
    LOG.info("Raft state machine is paused.");
  }

  /**
   * Unpause the StateMachine. This should be done after uploading new state to the StateMachine.
   */
  public synchronized void unpause() {
    LOG.info("Unpausing raft state machine.");
    if (mJournalApplier.isSuspended()) {
      LOG.warn("Journal should not be suspended while state machine is paused.");
    }
    getLifeCycle().startAndTransition(() -> {
      // nothing to do - just use this method to transition from PAUSE to RUNNING state
    });
    LOG.info("Raft state machine is unpaused.");
  }

  /**
   * Applies a journal entry commit to the state machine.
   * @param commit the commit
   */
  public synchronized void applyJournalEntryCommand(TransactionContext commit) {
    JournalEntry entry;
    try {
      entry = JournalEntry.parseFrom(
          commit.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e,
          "Encountered invalid journal entry in commit: %s.", commit);
      System.exit(-1);
      throw new IllegalStateException(e); // We should never reach here.
    }
    try {
      applyEntry(entry);
    } finally {
      Preconditions.checkState(commit.getLogEntry().getIndex() > mLastAppliedCommitIndex);
      mLastAppliedCommitIndex = commit.getLogEntry().getIndex();
    }
  }

  /**
   * Applies the journal entry, ignoring empty entries and expanding multi-entries.
   *
   * @param entry the entry to apply
   */
  private void applyEntry(JournalEntry entry) {
    if (LOG.isDebugEnabled()) {
      // This check is put behind the debug flag as the call to getAllFields creates
      // a map and is very expensive
      Preconditions.checkState(
          entry.getAllFields().size() <= 2
              || (entry.getAllFields().size() == 3 && entry.hasSequenceNumber()),
          "Raft journal entries should never set multiple fields in addition to sequence "
              + "number, but found %s",
          entry);
    }
    if (entry.getJournalEntriesCount() > 0) {
      // This entry aggregates multiple entries.
      for (JournalEntry e : entry.getJournalEntriesList()) {
        applyEntry(e);
      }
    } else if (entry.getSequenceNumber() < 0) {
      // Negative sequence numbers indicate special entries used to indicate that a new primary is
      // starting to serve.
      mLastPrimaryStartSequenceNumber = entry.getSequenceNumber();
    } else if (entry.toBuilder().clearSequenceNumber().build()
        .equals(JournalEntry.getDefaultInstance())) {
      // Ignore empty entries, they are created during snapshotting.
    } else {
      applySingleEntry(entry);
    }
  }

  @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
      justification = "All calls to applyJournalEntryCommand() are synchronized by ratis")
  private void applySingleEntry(JournalEntry entry) {
    if (mClosed) {
      return;
    }
    long newSN = entry.getSequenceNumber();
    if (newSN < mNextSequenceNumberToRead) {
      // This can happen due to retried writes. For example, if flushing [3, 4] fails, we will
      // retry, and the log may end up looking like [1, 2, 3, 4, 3, 4] if the original request
      // eventually succeeds. Once we've read the first "4", we must ignore the next two entries.
      LOG.debug("Ignoring duplicate journal entry with SN {} when next SN is {}", newSN,
          mNextSequenceNumberToRead);
      return;
    }
    if (newSN > mNextSequenceNumberToRead) {
      ProcessUtils.fatalError(LOG,
          "Unexpected journal entry. The next expected SN is %s, but"
              + " encountered an entry with SN %s. Full journal entry: %s",
          mNextSequenceNumberToRead, newSN, entry);
    }

    mNextSequenceNumberToRead++;
    if (!mIgnoreApplys) {
      mJournalApplier.processJournalEntry(entry);
    }
  }

  /**
   * Takes a snapshot of local state machine.
   * @param hasStateLock indicates whether this method call is guarded by a state lock
   * @return the index of last included entry, or {@link RaftLog#INVALID_LOG_INDEX} if it fails
   */
  public synchronized long takeLocalSnapshot(boolean hasStateLock) {
    // Snapshot format is [snapshotId, name1, bytes1, name2, bytes2, ...].
    if (mClosed) {
      SAMPLING_LOG.info("Skip taking snapshot because state machine is closed.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    RaftServer server = getServer().join(); // gets completed during initialization
    if (server.getLifeCycleState() != LifeCycle.State.RUNNING) {
      SAMPLING_LOG.info("Skip taking snapshot because raft server is not in running state: "
          + "current state is {}.", server.getLifeCycleState());
      return RaftLog.INVALID_LOG_INDEX;
    }
    if (mJournalApplier.isSuspended()) {
      SAMPLING_LOG.info("Skip taking snapshot while journal application is suspended.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    // Recheck mIsLeader (even though it was checked in #takeSnapshot) because mIsLeader is volatile
    // synchronized call to #isSnapshotting will prevent gaining leadership while this method is
    // executing
    if (mIsLeader && !hasStateLock) {
      LOG.warn("Tried to take local snapshot as leader without state lock");
      return RaftLog.INVALID_LOG_INDEX;
    }
    LOG.debug("Calling snapshot");
    Preconditions.checkState(!mSnapshotting, "Cannot call snapshot multiple times concurrently");
    mSnapshotting = true;
    TermIndex last = getLastAppliedTermIndex();

    File snapshotDir = getSnapshotDir(last.getTerm(), last.getIndex());
    if (!snapshotDir.isDirectory() && !snapshotDir.mkdir()) {
      return RaftLog.INVALID_LOG_INDEX;
    }
    try (Timer.Context ctx = MetricsSystem.timer(
             MetricKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_GENERATE_TIMER.getName()).time()) {
      Instant start = Instant.now();
      long snapshotId = mNextSequenceNumberToRead - 1;
      SingleEntryJournaled idWriter = new SnapshotIdJournaled();
      idWriter.processJournalEntry(JournalEntry.newBuilder().setSequenceNumber(snapshotId).build());
      CompletableFuture.allOf(Stream.concat(Stream.of(idWriter), getStateMachines().stream())
          .map(journaled -> journaled.writeToCheckpoint(snapshotDir, mJournalPool))
          .toArray(CompletableFuture[]::new))
          .join();
      mStorage.loadLatestSnapshot();
      mStorage.signalNewSnapshot();

      mLastSnapshotDurationMs = Duration.between(start, Instant.now()).toMillis();
      mLastSnapshotEntriesCount = mNextSequenceNumberToRead;
      return last.getIndex();
    } catch (Exception e) {
      LOG.error("error taking snapshot", e);
      return RaftLog.INVALID_LOG_INDEX;
    } finally {
      mSnapshotting = false;
    }
  }

  private void install(SnapshotInfo snapshot) {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected request to install a snapshot on a read-only journal state machine");
      return;
    }

    File snapshotDir = getSnapshotDir(snapshot.getTerm(), snapshot.getIndex());
    long snapshotId = 0L;
    try (Timer.Context ctx = MetricsSystem.timer(MetricKey
        .MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLAY_TIMER.getName()).time()) {
      Instant start = Instant.now();
      if (snapshotDir.isFile()) {
        LOG.info("Restoring from snapshot {} in old format", snapshot.getTermIndex());
        try (DataInputStream stream =  new DataInputStream(new FileInputStream(snapshotDir))) {
          snapshotId = stream.readLong();
          JournalUtils.restoreFromCheckpoint(new CheckpointInputStream(stream), getStateMachines());
        }
      } else {
        SingleEntryJournaled idReader = new SnapshotIdJournaled();
        CompletableFuture.allOf(Stream.concat(Stream.of(idReader), getStateMachines().stream())
                .map(journaled -> journaled.restoreFromCheckpoint(snapshotDir, mJournalPool))
                .toArray(CompletableFuture[]::new))
            .join();
        snapshotId = idReader.getEntry().getSequenceNumber();
      }
      mLastSnapshotReplayDurationMs = Duration.between(start, Instant.now()).toMillis();
    } catch (Exception e) {
      JournalUtils.handleJournalReplayFailure(LOG, e, "Failed to install snapshot: %s",
          snapshot.getTermIndex());
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_TOLERATE_CORRUPTION)) {
        return;
      }
    }

    if (snapshotId < mNextSequenceNumberToRead - 1) {
      LOG.warn("Installed snapshot for SN {} but next SN to read is {}", snapshotId,
          mNextSequenceNumberToRead);
    }
    mNextSequenceNumberToRead = snapshotId + 1;
    mLastSnapshotReplayEntriesCount = mNextSequenceNumberToRead;
    LOG.info("Successfully installed snapshot up to SN {}", snapshotId);
  }

  private File getSnapshotDir(long term, long index) {
    String dirName = SimpleStateMachineStorage.getSnapshotFileName(term, index);
    return new File(mStorage.getSnapshotDir(), dirName);
  }

  /**
   * Suspends applying to masters.
   *
   * When using suspend, the caller needs to provide a callback method as parameter. This callback
   * is invoked when the journal needs to reload and thus cannot suspend the state changes any
   * more. The callback should cancel any tasks that access the master states. After the callback
   * returns, the journal assumes that the states is no longer being accessed and will reload
   * immediately.
   *
   * @param interruptCallback a callback function to be called when the suspend is interrupted
   * @throws IOException if suspension fails
   */
  public synchronized void suspend(Runnable interruptCallback) throws IOException {
    LOG.info("Suspending raft state machine.");
    if (!getLifeCycleState().isRunning()) {
      throw new UnavailableException("Cannot suspend journal when state machine is paused.");
    }
    mJournalApplier.suspend();
    mInterruptCallback = interruptCallback;
    LOG.info("Raft state machine is suspended.");
  }

  /**
   * Resumes applying to masters.
   *
   * @throws IOException if resuming fails
   */
  public synchronized void resume() throws IOException {
    LOG.info("Resuming raft state machine");
    mInterruptCallback = null;
    if (mJournalApplier.isSuspended()) {
      mJournalApplier.resume();
      LOG.info("Raft state machine resumed");
    } else {
      LOG.info("Raft state machine is already resumed");
    }
  }

  /**
   * Initiates catching up of masters to given sequence.
   *
   * @param sequence the target sequence
   * @return the future to track when catching up is done
   */
  public synchronized CatchupFuture catchup(long sequence) {
    return mJournalApplier.catchup(sequence);
  }

  private List<Journaled> getStateMachines() {
    return StreamUtils.map(RaftJournal::getStateMachine, mJournals.values());
  }

  private synchronized void resetState() {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected call to resetState() on a read-only journal state machine");
      return;
    }
    mJournalApplier.close();
    mJournalApplier = new BufferedJournalApplier(mJournals,
        () -> mJournalSystem.getJournalSinks(null));
    for (RaftJournal journal : mJournals.values()) {
      journal.getStateMachine().resetState();
    }
  }

  /**
   * Upgrades the journal state machine to primary mode.
   *
   * @return the last sequence number read while in standby mode
   */
  public synchronized long upgrade() {
    // Resume the journal applier if was suspended.
    if (mJournalApplier.isSuspended()) {
      try {
        resume();
      } catch (IOException e) {
        ProcessUtils.fatalError(LOG, e, "State-machine failed to catch up after suspension.");
      }
    }
    mIgnoreApplys = true;
    return mNextSequenceNumberToRead - 1;
  }

  /**
   * @return the sequence number of the last entry applied to the state machine
   */
  public long getLastAppliedSequenceNumber() {
    return mNextSequenceNumberToRead - 1;
  }

  /**
   * @return the last primary term start sequence number applied to this state machine
   */
  public long getLastPrimaryStartSequenceNumber() {
    return mLastPrimaryStartSequenceNumber;
  }

  /**
   * @return the last raft log index which was applied to the state machine
   */
  public long getLastAppliedCommitIndex() {
    return mLastAppliedCommitIndex;
  }

  /**
   * @return whether the state machine is in the process of taking a snapshot
   */
  public synchronized boolean isSnapshotting() {
    return mSnapshotting;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId raftPeerId) {
    if (getGroupId() == groupMemberId.getGroupId()) {
      mIsLeader = groupMemberId.getPeerId() == raftPeerId;
      mJournalSystem.notifyLeadershipStateChanged(mIsLeader);
    } else {
      LOG.warn("Received notification for unrecognized group {}, current group is {}",
          groupMemberId.getGroupId(), getGroupId());
    }
  }

  /**
   * @return whether the journal is suspended
   */
  public synchronized boolean isSuspended() {
    return mJournalApplier.isSuspended();
  }
}
