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
import alluxio.master.journal.JournalReaderCreateOptions;
import alluxio.proto.journal.Journal;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.util.ArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalReader} based on UFS.
 */
@NotThreadSafe
public class UfsJournalReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UfsJournal mJournal;
  private final boolean mPrimary;
  private final long mCheckpointPeriodEntries;

  /**
   * The next edit log sequence number to read. This is not incremented when reading from
   * checkpoint.
   */
  private long mNextSequenceNumber;

  private JournalInputStream mInputStream;

  private final Queue<UfsJournalFile> mFilesToProcess;

  private final byte[] mBuffer = new byte[1024];

  private boolean mClosed;

  private class JournalInputStream implements Closeable {
    final UfsJournalFile mFile;
    /** The input stream that reads from a file. */
    final InputStream mStream;

    JournalInputStream(UfsJournalFile file) throws IOException {
      mFile = file;
      mStream = mJournal.getUfs().open(file.getLocation().toString());
    }

    boolean isDone() {
      return mFile.getEnd() == mNextSequenceNumber;
    }

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
    mFilesToProcess = new ArrayQueue<>();
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mNextSequenceNumber = options.getNextSequenceNumber();
    mPrimary = options.getPrimary();
    mCheckpointPeriodEntries = Configuration.getLong(
        PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mInputStream.close();
  }

  @Override
  public boolean shouldCheckpoint() throws IOException {
    if (mPrimary) {
      return false;
    }
    if (mNextSequenceNumber > mJournal.getNextLogSequenceToCheckpoint() &&
        mNextSequenceNumber % mCheckpointPeriodEntries == 0) {
      return true;
    }
    return false;
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
        LOG.warn("Skipping duplicate log entry {}.", entry);
      } else {
        throw new InvalidJournalEntryException(ExceptionMessage.JOURNAL_ENTRY_MISSING,
            mNextSequenceNumber, entry.getSequenceNumber());
      }
    }
  }

  /**
   * Reads an entry without checking sequence number. It does not mutate sequence number.
   * @return
   * @throws IOException
   * @throws InvalidJournalEntryException
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
        updateInputStream();
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

  private void updateInputStream() throws IOException {
    if (mInputStream != null && mInputStream.mFile.isIncompleteLog() || !mInputStream.isDone()) {
      return;
    }

    mInputStream.close();
    mInputStream = null;
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
        if (index < snapshot.mLogs.size() && snapshot.mLogs.get(index).getEnd() == checkpoint
            .getEnd()) {
          index++;
        }
      }
      for (; index < snapshot.mLogs.size(); ++index) {
        UfsJournalFile file = snapshot.mLogs.get(index);
        if (mPrimary && file.isIncompleteLog()) {
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
