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

package alluxio.master.journal.ufs;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.exception.JournalClosedException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.MasterJournalContext;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.URIUtils;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UFS-based journal.
 *
 * The journal is made up of 2 components:
 * - The checkpoint:  a snapshot of the master state
 * - The log entries: incremental entries to apply to the checkpoint.
 *
 * The journal log entries must be self-contained. Checkpoint is considered as a compaction of
 * a set of journal log entries. If the master does not do any checkpoint, the journal should
 * still be sufficient.
 *
 * Journal file structure:
 * journal_folder/version/logs/StartSequenceNumber-EndSequenceNumber
 * journal_folder/version/checkpoints/0-EndSequenceNumber
 * journal_folder/version/.tmp/random_id
 */
@ThreadSafe
public class UfsJournal implements Journal {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournal.class);
  /**
   * This is set to Long.MAX_VALUE such that the current log can be sorted after any other
   * completed logs.
   */
  public static final long UNKNOWN_SEQUENCE_NUMBER = Long.MAX_VALUE;
  /** The journal version. */
  public static final String VERSION = "v1";

  /** Directory for journal edit logs including the incomplete log file. */
  private static final String LOG_DIRNAME = "logs";
  /** Directory for committed checkpoints. */
  private static final String CHECKPOINT_DIRNAME = "checkpoints";
  /** Directory for temporary files. */
  private static final String TMP_DIRNAME = ".tmp";

  private final URI mLogDir;
  private final URI mCheckpointDir;
  private final URI mTmpDir;

  /** The location where this journal is stored. */
  private final URI mLocation;
  /** The state machine managed by this journal. */
  private final JournalEntryStateMachine mMaster;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;
  /** The amount of time to wait to pass without seeing a new journal entry when gaining primacy. */
  private final long mQuietPeriodMs;
  /** The current log writer. Null when in secondary mode. */
  private UfsJournalLogWriter mWriter;
  /** Asynchronous journal writer. */
  private AsyncJournalWriter mAsyncWriter;
  /**
   * Thread for tailing the journal, taking snapshots, and applying updates to the state machine.
   * Null when in primary mode.
   */
  private UfsJournalCheckpointThread mTailerThread;

  private enum State {
    SECONDARY, PRIMARY, CLOSED;
  }

  private State mState;

  /**
   * @return the ufs configuration to use for the journal operations
   */
  protected static UnderFileSystemConfiguration getJournalUfsConf() {
    Map<String, String> ufsConf =
        Configuration.getNestedProperties(PropertyKey.MASTER_JOURNAL_UFS_OPTION);
    return UnderFileSystemConfiguration.defaults().setUserSpecifiedConf(ufsConf);
  }

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   * @param stateMachine the state machine to manage
   * @param quietPeriodMs the amount of time to wait to pass without seeing a new journal entry when
   *        gaining primacy
   */
  public UfsJournal(URI location, JournalEntryStateMachine stateMachine, long quietPeriodMs) {
    this(location, stateMachine,
        UnderFileSystem.Factory.create(location.toString(), getJournalUfsConf()), quietPeriodMs);
  }

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   * @param stateMachine the state machine to manage
   * @param ufs the under file system
   * @param quietPeriodMs the amount of time to wait to pass without seeing a new journal entry when
   *        gaining primacy
   */
  UfsJournal(URI location, JournalEntryStateMachine stateMachine, UnderFileSystem ufs,
      long quietPeriodMs) {
    mLocation = URIUtils.appendPathOrDie(location, VERSION);
    mMaster = stateMachine;
    mUfs = ufs;
    mQuietPeriodMs = quietPeriodMs;

    mLogDir = URIUtils.appendPathOrDie(mLocation, LOG_DIRNAME);
    mCheckpointDir = URIUtils.appendPathOrDie(mLocation, CHECKPOINT_DIRNAME);
    mTmpDir = URIUtils.appendPathOrDie(mLocation, TMP_DIRNAME);
    mState = State.SECONDARY;
  }

  @Override
  public URI getLocation() {
    return mLocation;
  }

  /**
   * @param entry an entry to write to the journal
   */
  @VisibleForTesting
  synchronized void write(JournalEntry entry) throws IOException, JournalClosedException {
    writer().write(entry);
  }

  /**
   * Flushes the journal.
   */
  @VisibleForTesting
  public synchronized void flush() throws IOException, JournalClosedException {
    writer().flush();
  }

  @Override
  public synchronized JournalContext createJournalContext() throws UnavailableException {
    if (mState != State.PRIMARY) {
      // We throw UnavailableException here so that clients will retry with the next primary master.
      throw new UnavailableException("Failed to write to journal: journal is in state " + mState);
    }
    return new MasterJournalContext(mAsyncWriter);
  }

  private synchronized UfsJournalLogWriter writer() throws IOException {
    Preconditions.checkState(mState == State.PRIMARY,
        "Cannot write to the journal in state " + mState);
    return mWriter;
  }

  /**
   * Starts the journal in secondary mode.
   */
  public synchronized void start() throws IOException {
    mMaster.resetState();
    mTailerThread = new UfsJournalCheckpointThread(mMaster, this);
    mTailerThread.start();
  }

  /**
   * Transitions the journal from secondary to primary mode. The journal will apply the latest
   * journal entries to the state machine, then begin to allow writes.
   */
  public synchronized void gainPrimacy() throws IOException {
    Preconditions.checkState(mWriter == null, "writer must be null in secondary mode");
    Preconditions.checkState(mTailerThread != null,
        "tailer thread must not be null in secondary mode");
    mTailerThread.awaitTermination(true);
    long nextSequenceNumber = mTailerThread.getNextSequenceNumber();
    mTailerThread = null;
    nextSequenceNumber = catchUp(nextSequenceNumber);
    mWriter = new UfsJournalLogWriter(this, nextSequenceNumber);
    mAsyncWriter = new AsyncJournalWriter(mWriter);
    mState = State.PRIMARY;
  }

  /**
   * Transitions the journal from primary to secondary mode. The journal will no longer allow
   * writes, and the state machine is rebuilt from the journal and kept up to date.
   */
  public synchronized void losePrimacy() throws IOException {
    Preconditions.checkState(mState == State.PRIMARY, "unexpected state " + mState);
    Preconditions.checkState(mWriter != null, "writer thread must not be null in primary mode");
    Preconditions.checkState(mTailerThread == null, "tailer thread must be null in primary mode");
    mWriter.close();
    mWriter = null;
    mAsyncWriter = null;
    mMaster.resetState();
    mTailerThread = new UfsJournalCheckpointThread(mMaster, this);
    mTailerThread.start();
    mState = State.SECONDARY;
  }

  /**
   * @return the quiet period for this journal
   */
  public long getQuietPeriodMs() {
    return mQuietPeriodMs;
  }

  /**
   * @param readIncompleteLogs whether the reader should read the latest incomplete log
   * @return a reader for reading from the start of the journal
   */
  public UfsJournalReader getReader(boolean readIncompleteLogs) {
    return new UfsJournalReader(this, readIncompleteLogs);
  }

  /**
   * @param checkpointSequenceNumber the next sequence number after the checkpoint
   * @return a writer for writing a checkpoint
   */
  public UfsJournalCheckpointWriter getCheckpointWriter(long checkpointSequenceNumber)
      throws IOException {
    return new UfsJournalCheckpointWriter(this, checkpointSequenceNumber);
  }

  /**
   * @return the first log sequence number that hasn't yet been checkpointed
   */
  public long getNextSequenceNumberToCheckpoint() throws IOException {
    return UfsJournalSnapshot.getNextLogSequenceNumberToCheckpoint(this);
  }

  /**
   * @return whether the journal has been formatted
   */
  public boolean isFormatted() throws IOException {
    UfsStatus[] files = mUfs.listStatus(mLocation.toString());
    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    for (UfsStatus file : files) {
      if (file.getName().startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Formats the journal.
   */
  public void format() throws IOException {
    URI location = getLocation();
    LOG.info("Formatting {}", location);
    if (mUfs.isDirectory(location.toString())) {
      for (UfsStatus status : mUfs.listStatus(location.toString())) {
        String childPath = URIUtils.appendPathOrDie(location, status.getName()).toString();
        if (status.isDirectory()
            && !mUfs.deleteDirectory(childPath, DeleteOptions.defaults().setRecursive(true))
            || status.isFile() && !mUfs.deleteFile(childPath)) {
          throw new IOException(String.format("Failed to delete %s", childPath));
        }
      }
    } else if (!mUfs.mkdirs(location.toString())) {
      throw new IOException(String.format("Failed to create %s", location));
    }

    // Create a breadcrumb that indicates that the journal folder has been formatted.
    UnderFileSystemUtils.touch(mUfs, URIUtils.appendPathOrDie(location,
        Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX) + System.currentTimeMillis())
        .toString());
  }

  /**
   * @return the log directory location
   */
  @VisibleForTesting
  public URI getLogDir() {
    return mLogDir;
  }

  /**
   * @return the checkpoint directory location
   */
  URI getCheckpointDir() {
    return mCheckpointDir;
  }

  /**
   * @return the temporary directory location
   */
  URI getTmpDir() {
    return mTmpDir;
  }

  /**
   * @return the under file system instance
   */
  UnderFileSystem getUfs() {
    return mUfs;
  }

  /**
   * Reads and applies all journal entries starting from the specified sequence number.
   *
   * @param nextSequenceNumber the sequence number to continue catching up from
   * @return the next sequence number after the final sequence number read
   */
  private synchronized long catchUp(long nextSequenceNumber) {
    JournalReader journalReader = new UfsJournalReader(this, nextSequenceNumber, true);
    try {
      return catchUp(journalReader);
    } finally {
      try {
        journalReader.close();
      } catch (IOException e) {
        LOG.warn("Failed to close journal reader: {}", e.toString());
      }
    }
  }

  private long catchUp(JournalReader journalReader) {
    RetryPolicy retry =
        ExponentialTimeBoundedRetry.builder()
            .withInitialSleep(Duration.ofSeconds(1))
            .withMaxSleep(Duration.ofSeconds(10))
            .withMaxDuration(Duration.ofDays(365))
            .build();
    while (true) {
      JournalEntry entry;
      try {
        entry = journalReader.read();
      } catch (IOException e) {
        LOG.warn("{}: Failed to read from journal: {}", mMaster.getName(), e);
        if (retry.attempt()) {
          continue;
        }
        throw new RuntimeException(e);
      } catch (InvalidJournalEntryException e) {
        LOG.error("{}: Invalid journal entry detected.", mMaster.getName(), e);
        // We found an invalid journal entry, nothing we can do but crash.
        throw new RuntimeException(e);
      }
      if (entry == null) {
        return journalReader.getNextSequenceNumber();
      }
      try {
        mMaster.processJournalEntry(entry);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to process journal entry %s", entry), e);
      }
    }
  }

  @Override
  public String toString() {
    return "UfsJournal(" + mLocation + ")";
  }

  @Override
  public synchronized void close() throws IOException {
    if (mWriter != null) {
      mWriter.close();
      mWriter = null;
      mAsyncWriter = null;
    }
    if (mTailerThread != null) {
      mTailerThread.awaitTermination(false);
      mTailerThread = null;
    }
    mState = State.CLOSED;
  }
}
