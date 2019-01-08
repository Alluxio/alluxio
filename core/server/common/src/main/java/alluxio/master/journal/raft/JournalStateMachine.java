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

import alluxio.ProcessUtils;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

/**
 * A state machine representing the state of this journal system. Entries applied to this state
 * machine will be forwarded to the appropriate internal master.
 *
 * The state machine starts by resetting all state, then applying the entries offered by copycat.
 * When the master becomes primary, it should wait until the state machine is up to date and no
 * other primary master is serving, then call {@link #upgrade}. Once the state machine is upgraded,
 * it will ignore all entries appended by copycat because those entries are applied to primary
 * master state before being written to copycat.
 *
 * When the state machine takes a snapshot, every entry in the snapshot uses the largest sequence
 * number of all compacted entries. This way, installing the snapshot puts us at the same sequence
 * number as applying all of the individual entries represented by the snapshot.
 */
@ThreadSafe
public class JournalStateMachine extends StateMachine implements Snapshottable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalSystem.class);

  private final Map<String, RaftJournal> mJournals;
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

  /**
   * @param journals master journals; these journals are still owned by the caller, not by the
   *        journal state machine
   */
  public JournalStateMachine(Map<String, RaftJournal> journals) {
    mJournals = Collections.unmodifiableMap(journals);
    resetState();
    LOG.info("Initialized new journal state machine");
  }

  /**
   * Applies a journal entry commit to the state machine.
   *
   * This method is automatically discovered by the Copycat framework.
   *
   * @param commit the commit
   */
  public synchronized void applyJournalEntryCommand(Commit<JournalEntryCommand> commit) {
    JournalEntry entry;
    try {
      entry = JournalEntry.parseFrom(commit.command().getSerializedJournalEntry());
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e,
          "Encountered invalid journal entry in commit: {}.", commit);
      System.exit(-1);
      throw new IllegalStateException(e); // We should never reach here.
    }
    try {
      applyEntry(entry);
    } finally {
      Preconditions.checkState(commit.index() > mLastAppliedCommitIndex);
      mLastAppliedCommitIndex = commit.index();
      commit.close();
    }
  }

  /**
   * Applies the journal entry, ignoring empty entries and expanding multi-entries.
   *
   * @param entry the entry to apply
   */
  private void applyEntry(JournalEntry entry) {
    Preconditions.checkState(
        entry.getAllFields().size() <= 1
            || (entry.getAllFields().size() == 2 && entry.hasSequenceNumber()),
        "Raft journal entries should never set multiple fields in addition to sequence "
            + "number, but found %s",
        entry);
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
      justification = "All writes to mNextSequenceNumberToRead are synchronized")
  private synchronized void applySingleEntry(JournalEntry entry) {
    if (mClosed) {
      return;
    }
    long newSN = entry.getSequenceNumber();
    if (newSN < mNextSequenceNumberToRead) {
      LOG.info("Ignoring duplicate journal entry with SN {} when next SN is {}", newSN,
          mNextSequenceNumberToRead);
      return;
    }
    if (newSN > mNextSequenceNumberToRead) {
      ProcessUtils.fatalError(LOG,
          "Unexpected journal entry. The next expected SN is {}, but"
              + " encountered an entry with SN {}. Full journal entry: {}",
          mNextSequenceNumberToRead, newSN, entry);
    }

    mNextSequenceNumberToRead++;
    if (!mIgnoreApplys) {
      applyToMaster(entry);
    }
  }

  private synchronized void applyToMaster(JournalEntry entry) {
    String masterName;
    try {
      masterName = JournalEntryAssociation.getMasterForEntry(entry);
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Unrecognized journal entry: {}", entry);
      throw new IllegalStateException();
    }
    try {
      JournalEntryStateMachine master = mJournals.get(masterName).getStateMachine();
      LOG.trace("Applying entry to master {}: {} ", masterName, entry);
      master.processJournalEntry(entry);
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply journal entry to master {}. Entry: {}",
          masterName, entry);
    }
  }

  @Override
  public synchronized void snapshot(SnapshotWriter writer) {
    if (mClosed) {
      return;
    }
    LOG.debug("Calling snapshot");
    Preconditions.checkState(!mSnapshotting, "Cannot call snapshot multiple times concurrently");
    mSnapshotting = true;
    long start = System.currentTimeMillis();
    long snapshotSN = mNextSequenceNumberToRead - 1;
    try {
      for (RaftJournal journal : mJournals.values()) {
        for (Iterator<JournalEntry> it = journal.getStateMachine().getJournalEntryIterator(); it
            .hasNext();) {
          // All entries in a snapshot use the sequence number of the last entry included in the
          // snapshot
          JournalEntry entry = it.next().toBuilder().setSequenceNumber(snapshotSN).build();

          LOG.trace("Writing entry to snapshot: {}", entry);
          try {
            entry.writeDelimitedTo(new OutputStream() {
              @Override
              public void write(int b) throws IOException {
                writer.writeByte(b);
              }

              @Override
              public void write(byte[] b, int off, int len) {
                writer.write(b, off, len);
              }
            });
          } catch (IOException e) {
            ProcessUtils.fatalError(LOG, e,
                "Failed to take snapshot for master {}. Failed to write entry {}",
                journal.getStateMachine().getName(), entry);
          }
        }
      }
      LOG.info("Completed snapshot up to SN {} in {}ms", snapshotSN,
          System.currentTimeMillis() - start);
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to snapshot");
    }
    mSnapshotting = false;
  }

  @Override
  public synchronized void install(SnapshotReader snapshotReader) {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected request to install a snapshot on a read-only journal state machine");
      return;
    }
    resetState();
    JournalEntryStreamReader reader =
        new JournalEntryStreamReader(new SnapshotReaderStream(snapshotReader));

    JournalEntry entry = null;
    while (snapshotReader.hasRemaining()) {
      try {
        entry = reader.readEntry();
      } catch (IOException e) {
        ProcessUtils.fatalError(LOG, e, "Failed to install snapshot");
      }
      applyToMaster(entry);
    }
    long snapshotSN = entry != null ? entry.getSequenceNumber() : -1;
    if (snapshotSN < mNextSequenceNumberToRead - 1) {
      LOG.warn("Installed snapshot for SN {} but next SN to read is {}", snapshotSN,
          mNextSequenceNumberToRead);
    }
    mNextSequenceNumberToRead = snapshotSN + 1;
    LOG.info("Successfully installed snapshot up to SN {}", snapshotSN);
  }

  private synchronized void resetState() {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected call to resetState() on a read-only journal state machine");
      return;
    }
    for (RaftJournal journal : mJournals.values()) {
      journal.getStateMachine().resetState();
    }
  }

  /**
   * Upgrades the journal state machine to primary mode.
   *
   * @return the last sequence number read while in secondary mode
   */
  public synchronized long upgrade() {
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
   * @return whether the state machine is in the process of taking a snapshot
   */
  public boolean isSnapshotting() {
    return mSnapshotting;
  }

  /**
   * Closes the journal state machine, causing all further modification requests to be ignored.
   */
  public synchronized void close() {
    mClosed = true;
  }
}
