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

import alluxio.ProcessUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.master.journal.JournalReader;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalReader} that reads journal entries from a UFS. It can optionally
 * read after a given sequence number. By default, it starts from 0 sequence number.
 * If this reader runs in a primary master, it reads the incomplete log.
 * If this reader runs in a secondary master, it does not read the incomplete log.
 */
@NotThreadSafe
public final class UfsJournalReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UfsJournal mJournal;
  private final UnderFileSystem mUfs;

  /**
   * The next edit log sequence number to read. This is not incremented when reading from
   * the checkpoint.
   */
  private long mNextSequenceNumber;
  /** The input stream to read the journal entries. */
  private JournalInputStream mInputStream;
  /** A queue of files to be processed including checkpoint and logs. */
  private final Queue<UfsJournalFile> mFilesToProcess;

  private final boolean mReadIncompleteLog;

  /** Whether the reader is closed. */
  private boolean mClosed;

  /**
   * A simple wrapper that wraps the journal file and the input stream.
   */
  private class JournalInputStream implements Closeable {
    final UfsJournalFile mFile;
    /** The reader reading journal entries from the UfsJournalFile. */
    final JournalEntryStreamReader mReader;

    JournalInputStream(UfsJournalFile file) throws IOException {
      mFile = file;
      LOG.info("Reading journal file {}.", file.getLocation());
      mReader = new JournalEntryStreamReader(mUfs.open(file.getLocation().toString(),
          OpenOptions.defaults().setRecoverFailedOpen(true)));
    }

    /**
     * @return whether we have finished reading the current file
     */
    boolean isDone() {
      return mFile.getEnd() == mNextSequenceNumber;
    }

    @Override
    public void close() throws IOException {
      mReader.close();
    }
  }

  /**
   * Creates a new instance of {@link UfsJournalReader}.
   *
   * @param journal the handle to the journal
   * @param readIncompleteLog whether to read incomplete logs
   */
  UfsJournalReader(UfsJournal journal, boolean readIncompleteLog) {
    this(journal, 0, readIncompleteLog);
  }

  /**
   * Creates a new instance of {@link UfsJournalReader}.
   *
   * @param journal the handle to the journal
   * @param startSequenceId the sequence ID to start reading from
   * @param readIncompleteLog whether to read incomplete logs
   */
  public UfsJournalReader(UfsJournal journal, long startSequenceId, boolean readIncompleteLog) {
    mFilesToProcess = new ArrayDeque<>();
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = mJournal.getUfs();
    mNextSequenceNumber = startSequenceId;
    mReadIncompleteLog = readIncompleteLog;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    if (mInputStream != null) {
      mInputStream.close();
    }
  }

  @Override
  public long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  @Override
  public Journal.JournalEntry read() throws IOException, InvalidJournalEntryException {
    while (true) {
      Journal.JournalEntry entry;
      try {
        entry = readInternal();
      } catch (IOException e) {
        throw new IOException(String
            .format("Failed to read from journal: %s error: %s", mJournal.getLocation(),
                e.getMessage()), e);
      }
      if (entry == null) {
        return null;
      }
      if (mInputStream.mFile.isCheckpoint()) {
        return entry;
      } else if (entry.getSequenceNumber() == mNextSequenceNumber) {
        mNextSequenceNumber++;
        return entry;
      } else if (entry.getSequenceNumber() < mNextSequenceNumber) {
        // This can happen in the following two scenarios:
        // 1. The primary master failed when renaming the current log to completed log which might
        //    result in duplicate logs.
        // 2. The first completed log after the checkpoint's last sequence number might contains
        //    some duplicate entries with the checkpoint.
        LOG.debug("Skipping duplicate log entry {} (next sequence number: {}).", entry,
            mNextSequenceNumber);
      } else {
        throw new IllegalStateException(ExceptionMessage.JOURNAL_ENTRY_MISSING.getMessage(
            mNextSequenceNumber, entry.getSequenceNumber()));
      }
    }
  }

  /**
   * Reads the next journal entry.
   *
   * @return the journal entry, null if no journal entry is found
   */
  private Journal.JournalEntry readInternal() throws IOException {
    updateInputStream();
    if (mInputStream == null) {
      return null;
    }
    JournalEntry entry = mInputStream.mReader.readEntry();
    if (entry != null) {
      return entry;
    }
    // If this is the checkpoint file, we need to reset the sequence number to update the stream
    // because the sequence number in the checkpoint entries is not in the same space as the
    // sequence number in the edit logs.
    if (mInputStream.mFile.isCheckpoint()) {
      mNextSequenceNumber = mInputStream.mFile.getEnd();
      return readInternal();
    } else if (mInputStream.mFile.isIncompleteLog()) {
      // Incomplete logs may end early.
      return null;
    } else {
      Preconditions.checkState(mInputStream.mFile.isCompletedLog(),
          "Expected log to be either checkpoint, incomplete, or complete");
      ProcessUtils.fatalError(LOG, "Journal entry %s was truncated", mNextSequenceNumber);
      return null;
    }
  }

  /**
   * Updates the journal input stream by closing the current journal input stream if it is done and
   * opening a new one.
   */
  private void updateInputStream() throws IOException {
    if (mInputStream != null && (mInputStream.mFile.isIncompleteLog() || !mInputStream.isDone())) {
      return;
    }

    if (mInputStream != null) {
      mInputStream.close();
      mInputStream = null;
    }
    if (mFilesToProcess.isEmpty()) {
      UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
      if (snapshot.getCheckpoints().isEmpty() && snapshot.getLogs().isEmpty()) {
        return;
      }

      int index = 0;
      if (!snapshot.getCheckpoints().isEmpty()) {
        UfsJournalFile checkpoint = snapshot.getLatestCheckpoint();
        if (mNextSequenceNumber < checkpoint.getEnd()) {
          mFilesToProcess.add(checkpoint);
          // Reset the sequence number to 0 because it is not supported to read from checkpoint with
          // an offset. This can only happen in the following scenario:
          // 1. Read checkpoint to SN1, then optionally read completed logs to SN2 (>= SN1).
          // 2. A new checkpoint is written to SN3 (> SN2).
          // 3. Resume reading from SN2.
          mNextSequenceNumber = 0;
        }
        for (; index < snapshot.getLogs().size(); index++) {
          UfsJournalFile file = snapshot.getLogs().get(index);
          if (file.getEnd() > checkpoint.getEnd()) {
            break;
          }
        }
        // index now points to the first log with mEnd > checkpoint.mEnd.
      }
      for (; index < snapshot.getLogs().size(); index++) {
        UfsJournalFile file = snapshot.getLogs().get(index);
        if ((!mReadIncompleteLog && file.isIncompleteLog())
            || mNextSequenceNumber >= file.getEnd()) {
          continue;
        }
        mFilesToProcess.add(snapshot.getLogs().get(index));
      }
    }

    if (!mFilesToProcess.isEmpty()) {
      mInputStream = new JournalInputStream(mFilesToProcess.poll());
    }
  }
}
