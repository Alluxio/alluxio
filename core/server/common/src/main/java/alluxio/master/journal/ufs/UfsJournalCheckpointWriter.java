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
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.options.JournalWriterCreateOptions;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link JournalWriter} based on UFS.
 */
@ThreadSafe
public final class UfsJournalCheckpointWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalCheckpointWriter.class);

  private final UfsJournal mJournal;

  private final UfsJournalFile mCheckpointFile;
  private final OutputStream mTmpCheckpointStream;
  private final URI mTmpCheckpointFileLocation;

  private long mNextSequenceNumber;

  private boolean mClosed;

  /**
   * Creates a new instance of {@link UfsJournalCheckpointWriter}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalCheckpointWriter(UfsJournal journal, JournalWriterCreateOptions options)
      throws IOException {
    mJournal = Preconditions.checkNotNull(journal);

    mTmpCheckpointFileLocation = mJournal.getTemporaryCheckpointFileLocation();
    mTmpCheckpointStream = mJournal.getUfs().create(mTmpCheckpointFileLocation.toString());
    mCheckpointFile = UfsJournalFile.createCheckpointFile(
        mJournal.getCheckpointOrLogFileLocation(0, options.getNextSequenceNumber(), true),
        options.getNextSequenceNumber());
  }

  @Override
  public synchronized void write(JournalEntry entry) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
    }
    try {
      entry.toBuilder().setSequenceNumber(mNextSequenceNumber).build()
          .writeDelimitedTo(mTmpCheckpointStream);
    } catch (IOException e) {
      throw e;
    }
    mNextSequenceNumber++;
  }

  @Override
  public synchronized void flush() throws IOException {
    throw new UnsupportedOperationException("UfsJournalCheckpointWriter#flush is not supported.");
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mTmpCheckpointStream.close();

    // Delete the temporary checkpoint if there is a newer checkpoint committed.
    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    if (snapshot != null && !snapshot.mCheckpoints.isEmpty()) {
      UfsJournalFile checkpoint =
          snapshot.mCheckpoints.get(snapshot.mCheckpoints.size() - 1);
      if (mNextSequenceNumber <= checkpoint.getEnd()) {
        mJournal.getUfs().deleteFile(mTmpCheckpointFileLocation.toString());
        return;
      }
    }

    String dst = mCheckpointFile.getLocation().toString();
    try {
      mJournal.getUfs().renameFile(mTmpCheckpointFileLocation.toString(), dst);
    } catch (IOException e) {
      if (!mJournal.getUfs().exists(dst)) {
        LOG.warn("Failed to commit checkpoint from {} to {} with error {}.",
            mTmpCheckpointFileLocation, dst, e.getMessage());
      }
      try {
        mJournal.getUfs().deleteFile(mTmpCheckpointFileLocation.toString());
      } catch (IOException ee) {
        LOG.warn("Failed to clean up temporary checkpoint {} at {}.", mTmpCheckpointFileLocation,
            mNextSequenceNumber);
      }
      throw e;
    }
  }

  @Override
  public synchronized void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    mTmpCheckpointStream.close();
    if (mJournal.getUfs().exists(mTmpCheckpointFileLocation.toString())) {
      mJournal.getUfs().deleteFile(mTmpCheckpointFileLocation.toString());
    }
  }
}
