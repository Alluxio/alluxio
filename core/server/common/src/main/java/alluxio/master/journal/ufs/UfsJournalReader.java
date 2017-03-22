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
import alluxio.master.journal.JournalInputStream;
import alluxio.master.journal.JournalReader;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalReader} based on UFS.
 */
@NotThreadSafe
public class UfsJournalReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UfsJournal mJournal;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  /** The next sequence number to read. */
  private long mSequenceNumber;

  private InputStream mInputStream;
  private boolean isReadingFromInCompleteLog;

  private final byte[] mBuffer = new byte[1024];

  /**
   * Creates a new instance of {@link UfsJournalReader}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalReader(UfsJournal journal) {
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = UnderFileSystem.Factory.get(mJournal.getLocation().toString());
  }

  @Override
  public Journal.JournalEntry read() throws IOException, InvalidJournalEntryException {

  }

  private Journal.JournalEntry readInternal() throws IOException, InvalidJournalEntryException {
    updateInputStream();

    int firstByte = mInputStream.read();
    if (firstByte == -1) {
      return null;
    }
    // All journal entries start with their size in bytes written as a varint.
    int size;
    try {
      size = ProtoUtils.readRawVarint32(firstByte, mInputStream);
    } catch (IOException e) {
      LOG.warn("Journal entry was truncated in the size portion.");
      if (isReadingFromInCompleteLog && ProtoUtils.isTruncatedMessageException(e)) {
        return null;
      }
      throw e;
    }
    byte[] buffer = size <= mBuffer.length ? mBuffer : new byte[size];
    // Total bytes read so far for journal entry.
    int totalBytesRead = 0;
    while (totalBytesRead < size) {
      // Bytes read in last read request.
      int latestBytesRead = mInputStream.read(buffer, totalBytesRead, size - totalBytesRead);
      if (latestBytesRead < 0) {
        break;
      }
      totalBytesRead += latestBytesRead;
    }
    if (totalBytesRead < size) {
      LOG.warn("Journal entry was truncated. Expected to read " + size + " bytes but only got "
          + totalBytesRead);
      if (!isReadingFromInCompleteLog) {
        throw new InvalidJournalEntryException(
            ExceptionMessage.JOURNAL_ENTRY_TRUNCATED_UNEXPECTEDLY, mSequenceNumber);
      }
      return null;
    }

    Journal.JournalEntry
        entry = Journal.JournalEntry.parseFrom(new ByteArrayInputStream(buffer, 0, size));
    // TODO(peis): Check with Andrew to make sure this check is ok.
    Preconditions.checkNotNull(entry);
    if (mSequenceNumber != entry.getSequenceNumber()) {
      throw new InvalidJournalEntryException(ExceptionMessage.JOURNAL_ENTRY_MISSING,
          mSequenceNumber, entry.getSequenceNumber());
    }
    mSequenceNumber++;
    return entry;
  }

  private void updateInputStream() throws IOException {

  }

  private class JournalInputStream {
    public final InputStream mInputStream;
    public final long mStart;
    public final long mEnd;
  }


  @Override
  public JournalInputStream getCheckpointInputStream() throws IOException {
    if (mCheckpointRead) {
      throw new IOException("Checkpoint file has already been read.");
    }
    mCheckpointOpenedTime = getCheckpointLastModifiedTimeMs();

    LOG.info("Opening journal checkpoint file: {}", mCheckpoint);
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(mUfs.open(mCheckpoint.toString()));

    mCheckpointRead = true;
    return jis;
  }

  @Override
  public JournalInputStream getNextInputStream() throws IOException {
    if (!mCheckpointRead) {
      throw new IOException("Must read the checkpoint file before getting input stream.");
    }
    if (getCheckpointLastModifiedTimeMs() != mCheckpointOpenedTime) {
      throw new IOException("Checkpoint file has been updated. This reader is no longer valid.");
    }
    URI currentLog = mJournal.getCompletedLog(mCurrentLogNumber);
    if (!mUfs.isFile(currentLog.toString())) {
      LOG.debug("Journal log file: {} does not exist yet.", currentLog);
      return null;
    }
    // Open input stream from the current log file.
    LOG.info("Opening journal log file: {}", currentLog);
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(mUfs.open(currentLog.toString()));

    // Increment the log file number.
    mCurrentLogNumber++;
    return jis;
  }

  @Override
  public long getCheckpointLastModifiedTimeMs() throws IOException {
    if (!mUfs.isFile(mCheckpoint.toString())) {
      throw new IOException("Checkpoint file " + mCheckpoint + " does not exist.");
    }
    mCheckpointLastModifiedTime = mUfs.getModificationTimeMs(mCheckpoint.toString());
    return mCheckpointLastModifiedTime;
  }
}
