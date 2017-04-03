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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.options.JournalReaderCreateOptions;
import alluxio.proto.journal.Journal;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalReader} that reads journal entries from a UFS. It can optionally
 * read after a given sequence number. By default, it starts from 0 sequence number.
 * If this reader runs in a primary master, it reads the incomplete log.
 * If this reader runs in a secondary master, it does not read the incomplete log.
 */
@NotThreadSafe
class UfsJournalReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UfsJournal mJournal;
  /** Whether the reader runs in a primary master. */
  private final boolean mPrimary;
  private final long mCheckpointPeriodEntries;
  /**
   * The next edit log sequence number to read. This is not incremented when reading from
   * the checkpoint.
   */
  private long mNextSequenceNumber;
  /** The input stream to read the journal entries. */
  private JournalInputStream mInputStream;
  /** A queue of files to be processed including checkpoint and logs. */
  private final Queue<UfsJournalFile> mFilesToProcess;
  /** Buffer used to read from the file. */
  private final byte[] mBuffer = new byte[1024];

  /** Whether the reader is closed. */
  private boolean mClosed;

  /**
   * A simple wrapper that wraps the journal file and the input stream.
   */
  private class JournalInputStream implements Closeable {
    final UfsJournalFile mFile;
    /** The input stream that reads from a file. */
    final InputStream mStream;

    JournalInputStream(UfsJournalFile file) throws IOException {
      mFile = file;
      mStream = mJournal.getUfs().open(file.getLocation().toString());
    }

    /**
     * @return whether we have finished reading the current file
     */
    boolean isDone() {
      return mFile.getEnd() == mNextSequenceNumber;
    }

    @Override
    public void close() throws IOException {
      mStream.close();
    }
  }

  /**
   * Creates a new instance of {@link UfsJournalReader}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalReader(UfsJournal journal, JournalReaderCreateOptions options) {
    mFilesToProcess = new ArrayDeque<>();
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mNextSequenceNumber = options.getNextSequenceNumber();
    mPrimary = options.isPrimary();
    mCheckpointPeriodEntries = Configuration.getLong(
        PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);
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
  public boolean shouldCheckpoint() throws IOException {
    if (mPrimary) {
      return false;
    }
    if (mNextSequenceNumber > mJournal.getNextLogSequenceToCheckpoint()
        && mNextSequenceNumber % mCheckpointPeriodEntries == 0) {
      return true;
    }
    return false;
  }

  @Override
  public long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  @Override
  public Journal.JournalEntry read() throws IOException, InvalidJournalEntryException {
    while (true) {
      Journal.JournalEntry entry = readInternal();
      if (entry == null) {
        return null;
      }
      if (mInputStream.mFile.isCheckpoint()) {
        return entry;
      }
      if (entry.getSequenceNumber() == mNextSequenceNumber) {
        mNextSequenceNumber++;
        return entry;
      }
      if (entry.getSequenceNumber() < mNextSequenceNumber) {
        // This can happen in the following two scenarios:
        // 1. The primary master failed when renaming the current log to completed log which might
        //    result in duplicate logs.
        // 2. The first completed log after the checkpoint's last sequence number might contains
        //    some duplicate entries with the checkpoint.
        LOG.debug("Skipping duplicate log entry {}.", entry);
      } else {
        throw new InvalidJournalEntryException(ExceptionMessage.JOURNAL_ENTRY_MISSING,
            mNextSequenceNumber, entry.getSequenceNumber());
      }
    }
  }

  /**
   * The real read implementation that reads a journal entry from a journal file.
   *
   * @return the journal entry, null if no journal entry is found
   * @throws IOException if any I/O errors occur
   * @throws InvalidJournalEntryException if the journal entry found is invalid
   */
  private Journal.JournalEntry readInternal() throws IOException, InvalidJournalEntryException {
    updateInputStream();
    if (mInputStream == null) {
      return null;
    }

    int firstByte = mInputStream.mStream.read();
    if (firstByte == -1) {
      // If this is the checkpoint file, we need to reset the sequence number to update the stream
      // because the sequence number in the checkpoint entries is not in the same space as the
      // sequence number in the edit logs.
      if (mInputStream.mFile.isCheckpoint()) {
        mNextSequenceNumber = mInputStream.mFile.getEnd();
        return readInternal();
      }

      if (!mInputStream.mFile.isIncompleteLog()) {
        throw new InvalidJournalEntryException(
            ExceptionMessage.JOURNAL_ENTRY_TRUNCATED_UNEXPECTEDLY, mNextSequenceNumber);
      }
      return null;
    }
    // All journal entries start with their size in bytes written as a varint.
    int size;
    try {
      size = ProtoUtils.readRawVarint32(firstByte, mInputStream.mStream);
    } catch (IOException e) {
      LOG.warn("Journal entry was truncated in the size portion.");
      if (mInputStream.mFile.isIncompleteLog() && ProtoUtils.isTruncatedMessageException(e)) {
        return null;
      }
      throw e;
    }
    byte[] buffer = size <= mBuffer.length ? mBuffer : new byte[size];
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead =
          mInputStream.mStream.read(buffer, totalBytesRead, size - totalBytesRead);
      if (latestBytesRead < 0) {
        break;
      }
      totalBytesRead += latestBytesRead;
    }
    if (totalBytesRead < size) {
      LOG.warn("Journal entry was truncated. Expected to read " + size + " bytes but only got "
          + totalBytesRead);
      if (!mInputStream.mFile.isIncompleteLog()) {
        throw new InvalidJournalEntryException(
            ExceptionMessage.JOURNAL_ENTRY_TRUNCATED_UNEXPECTEDLY, mNextSequenceNumber);
      }
      return null;
    }

    Journal.JournalEntry entry =
        Journal.JournalEntry.parseFrom(new ByteArrayInputStream(buffer, 0, size));
    return entry;
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
      UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
      // Remove incomplete log if this is a secondary master.
      if (snapshot.mCheckpoints.isEmpty() && snapshot.mLogs.isEmpty()) {
        return;
      }

      int index = 0;
      if (mNextSequenceNumber == 0 && !snapshot.mCheckpoints.isEmpty()) {
        UfsJournalFile checkpoint = snapshot.mCheckpoints.get(snapshot.mCheckpoints.size() - 1);
        mFilesToProcess.add(checkpoint);
        // index points to the log with mEnd >= checkpoint.mEnd.
        index = Collections.binarySearch(snapshot.mLogs, checkpoint);
        if (index >= 0) {
          index++;
        } else {
          // Find the insertion point.
          index = -index - 1;
        }
      }
      for (; index < snapshot.mLogs.size(); ++index) {
        UfsJournalFile file = snapshot.mLogs.get(index);
        if ((!mPrimary && file.isIncompleteLog()) || mNextSequenceNumber >= file.getEnd()) {
          continue;
        }
        mFilesToProcess.add(snapshot.mLogs.get(index));
      }
    }

    if (!mFilesToProcess.isEmpty()) {
      mInputStream = new JournalInputStream(mFilesToProcess.poll());
    }
  }
}
