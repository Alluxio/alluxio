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

package alluxio.master.journal;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A journal system for storing and applying journal entries.
 *
 * To use the journal system, first create per-state-machine journals with the
 * {@link #createJournal(JournalEntryStateMachine)} method. Once all state machines are added,
 * {@link #start()} the journal system. The journal system starts in secondary mode, meaning it will
 * not accept writes, but will apply journal entries to keep state machine states up to date with
 * previously written journal entries.
 *
 * To enable writes, the journal system may be changed to
 * primary mode, where all state machines will first be caught up fully with the latest journal
 * updates, then begin to accept writes. Note that as a performance optimization, the journal system
 * does not apply journal entries to state machines while in primary mode. Instead, the state
 * machine states must be directly modified by RPC handlers.
 *
 * The journal system may also be changed from primary to secondary mode. This transition is done by
 * resetting all state machines and re-building them by catching up on the journal.
 *
 * Example usage:
 *
 * <pre>
 * JournalSystem journalSystem = new JournalSystemImpl();
 * if (!journalSystem.isFormatted()) {
 *   journalSystem.format();
 * }
 * Journal blockMasterJournal = journalSystem.createJournal(blockMaster);
 * Journal fileSystemMasterJournal = journalSystem.createJournal(fileSystemMaster);
 *
 * // The journal system always starts in secondary mode. It must be transitioned to primary mode
 * // before it can write entries.
 * journalSystem.start();
 * journalSystem.setPrimary(true);
 *
 * try (JournalContext c = blockMasterJournal.createJournalContext()) {
 *   c.append(exampleBlockJournalEntry);
 * }
 * // At this point, the journal entry is persistently committed to the journal and will be applied
 * // asynchronously to the in-memory state of all secondary masters.
 * try (JournalContext c = fileSystemMasterJournal.createJournalContext()) {
 *   c.append(exampleFileSystemJournalEntry);
 * }
 * // Transition to a secondary journal. In this mode, the journal will apply entries to the masters
 * // as they are committed to the log.
 * journalSystem.setPrimary(false);
 * </pre>
 */
@ThreadSafe
public interface JournalSystem {

  /**
   * The mode of the journal system. Journal systems begin in SECONDARY mode by default. The
   * {@link #gainPrimacy()} and {@link #losePrimacy()} methods may be used to transition between
   * journal modes.
   */
  enum Mode {
    /**
     * In this mode, journal entries may be written. Written journal entries will not be applied to
     * journals' state machines.
     */
    PRIMARY,
    /**
     * In this mode, journal entries may not be written. Journal entries written by the primary will
     * be applied to journals' state machines.
     */
    SECONDARY
  }

  /**
   * Creates a journal for the given state machine.
   *
   * The returned journal can create journal contexts for writing journal entries. However, no
   * entries may be written until the journal system has been started, and entries may only be
   * written when the journal system is in PRIMARY mode.
   *
   * When the journal is started in secondary mode, it will call
   * {@link JournalEntryStateMachine#processJournalEntry(JournalEntry)} and
   * {@link JournalEntryStateMachine#resetState()} to keep the state machine's state in sync with
   * the entries written to the journal.
   *
   * @param stateMachine the state machine to create the journal for
   * @return a new instance of {@link Journal}
   */
  Journal createJournal(JournalEntryStateMachine stateMachine);

  /**
   * Starts the journal system.
   *
   * All journals must be created before starting the journal system. This method will block until
   * the journal system is successfully started. The journal always starts in secondary mode.
   */
  void start() throws InterruptedException, IOException;

  /**
   * Stops the journal system.
   */
  void stop() throws InterruptedException, IOException;

  /**
   * Transitions the journal to primary mode.
   */
  void gainPrimacy();

  /**
   * Transitions the journal to secondary mode.
   */
  void losePrimacy();

  /**
   * Formats the journal system.
   */
  void format() throws IOException;

  /**
   * @return whether the journal system has been formatted
   */
  boolean isFormatted() throws IOException;

  /**
   * Returns whether the journal is formatted and has not had any entries written to it yet. This
   * can only be determined when the journal system is in primary mode because entries are written
   * to the primary first.
   *
   * @return whether the journal system is freshly formatted
   */
  boolean isEmpty();

  /**
   * Builder for constructing a journal system.
   */
  class Builder {
    private URI mLocation;
    private long mQuietTimeMs =
        Configuration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS);

    /**
     * Creates a new journal system builder.
     */
    public Builder() {}

    /**
     * @param location the location for the journal system
     * @return the updated builder
     */
    public Builder setLocation(URI location) {
      mLocation = location;
      return this;
    }

    /**
     * @param quietTimeMs before upgrading from SECONDARY to PRIMARY mode, the journal will wait
     *        until this duration has passed without any journal entries being written.
     * @return the updated builder
     */
    public Builder setQuietTimeMs(long quietTimeMs) {
      mQuietTimeMs = quietTimeMs;
      return this;
    }

    /**
     * @return a journal system
     */
    public JournalSystem build() {
      JournalType journalType =
          Configuration.getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
      switch (journalType) {
        case NOOP:
          return new NoopJournalSystem();
        case UFS:
          return new UfsJournalSystem(mLocation, mQuietTimeMs);
        case EMBEDDED:
          ServiceType serviceType;
          if (CommonUtils.PROCESS_TYPE.get().equals(CommonUtils.ProcessType.MASTER)) {
            serviceType = ServiceType.MASTER_RAFT;
          } else {
            // We might reach here during journal formatting. In that case the journal system is
            // never started, so any value of serviceType is fine.
            serviceType = ServiceType.JOB_MASTER_RAFT;
          }
          return RaftJournalSystem.create(RaftJournalConfiguration.defaults(serviceType)
                  .setPath(new File(mLocation.getPath())));
        default:
          throw new IllegalStateException("Unrecognized journal type: " + journalType);
      }
    }
  }
}
