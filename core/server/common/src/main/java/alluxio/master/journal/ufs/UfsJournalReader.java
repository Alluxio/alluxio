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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.options.JournalReaderOptions;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
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
final class UfsJournalReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UfsJournal mJournal;
  private final UnderFileSystem mUfs;
  /** Whether the reader runs in a primary master. */
  private final boolean mPrimary;

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
      LOG.info("Reading journal file {}.", file.getLocation());
      mStream = mUfs.open(file.getLocation().toString());
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
  UfsJournalReader(UfsJournal journal, JournalReaderOptions options) {
    mFilesToProcess = new ArrayDeque<>();
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = mJournal.getUfs();
    mNextSequenceNumber = options.getNextSequenceNumber();
    mPrimary = options.isPrimary();
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
      Journal.JournalEntry entry = readInternal();
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
        throw new InvalidJournalEntryException(ExceptionMessage.JOURNAL_ENTRY_MISSING,
            mNextSequenceNumber, entry.getSequenceNumber());
      }
    }
  }

  /**
   * The real read implementation that reads a journal entry from a journal file.
   *
   * @return the journal entry, null if no journal entry is found
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
        if ((!mPrimary && file.isIncompleteLog()) || mNextSequenceNumber >= file.getEnd()) {
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
