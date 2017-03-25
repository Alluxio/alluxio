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
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.URIUtils;

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

  private final OutputStream mTmpCheckpointStream;
  private final URI mTmpCheckpointFileLocation;

  private long mSequenceNumber;

  private boolean mClosed;

  /**
   * Creates a new instance of {@link UfsJournalCheckpointWriter}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalCheckpointWriter(UfsJournal journal) throws IOException {
    mJournal = Preconditions.checkNotNull(journal);

    mTmpCheckpointFileLocation = mJournal.getTemporaryCheckpointFileLocation();
    mTmpCheckpointStream = mJournal.getUfs().create(mTmpCheckpointFileLocation.toString());
  }

  @Override
  public synchronized void write(JournalEntry entry) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
    }
    try {
      entry.toBuilder().setSequenceNumber(mSequenceNumber).build()
          .writeDelimitedTo(mTmpCheckpointStream);
    } catch (IOException e) {
      throw e;
    }
    mSequenceNumber++;
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
      UfsJournal.JournalFile checkpoint =
          snapshot.mCheckpoints.get(snapshot.mCheckpoints.size() - 1);
      if (mSequenceNumber <= checkpoint.mEnd) {
        mJournal.getUfs().deleteFile(mTmpCheckpointFileLocation.toString());
        return;
      }
    }

    URI dst =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), Long.toHexString(mSequenceNumber));
    try {
      mJournal.getUfs().renameFile(mTmpCheckpointFileLocation.toString(), dst.toString());
    } catch (IOException e) {
      if (!mJournal.getUfs().exists(dst.toString())) {
        LOG.warn("Failed to commit checkpoint from {} to {} with error {}.",
            mTmpCheckpointFileLocation, dst, e.getMessage());
      }
      try {
        mJournal.getUfs().deleteFile(mTmpCheckpointFileLocation.toString());
      } catch (IOException ee) {
        LOG.warn("Failed to clean up temporary checkpoint {} at {}.", mTmpCheckpointFileLocation,
            mSequenceNumber);
      }
      throw e;
    }
  }
}
