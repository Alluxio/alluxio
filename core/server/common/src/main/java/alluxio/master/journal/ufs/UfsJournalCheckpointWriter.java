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
import alluxio.master.journal.options.JournalWriterOptions;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalWriter} that writes checkpoint to a UFS. The secondary masters
 * uses this to periodically create new checkpoints.
 *
 * It first writes checkpoint to a temporary location. After it is done with writing the temporary
 * checkpoint, commit it by renaming the temporary checkpoint to the final location. If the same
 * checkpoint has already been created by another secondary master, the checkpoint is aborted.
 */
@NotThreadSafe
final class UfsJournalCheckpointWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalCheckpointWriter.class);

  private final UfsJournal mJournal;
  private final UnderFileSystem mUfs;

  /** The checkpoint file to be committed to. */
  private final UfsJournalFile mCheckpointFile;
  /** The location for the temporary checkpoint. */
  private final URI mTmpCheckpointFileLocation;
  /** The output stream to the temporary checkpoint file. */
  private final OutputStream mTmpCheckpointStream;

  /**
   * The sequence number for the next journal entry to be written to the checkpoint. Note that this
   * always starts with 0 and is not necessarily the same as the sequence number in edit logs.
   */
  private long mNextSequenceNumber;

  /** Whether this journal writer is closed. */
  private boolean mClosed;

  /**
   * Creates a new instance of {@link UfsJournalCheckpointWriter}.
   *
   * @param journal the handle to the journal
   * @param options the options to create the journal writer
   */
  UfsJournalCheckpointWriter(UfsJournal journal, JournalWriterOptions options)
      throws IOException {
    mJournal = Preconditions.checkNotNull(journal);
    mUfs = mJournal.getUfs();

    mTmpCheckpointFileLocation = UfsJournalFile.encodeTemporaryCheckpointFileLocation(mJournal);
    mTmpCheckpointStream = mUfs.create(mTmpCheckpointFileLocation.toString());
    mCheckpointFile = UfsJournalFile.createCheckpointFile(
        UfsJournalFile.encodeCheckpointFileLocation(mJournal, options.getNextSequenceNumber()),
        options.getNextSequenceNumber());
  }

  @Override
  public void write(JournalEntry entry) throws IOException {
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
  public void flush() throws IOException {
    mTmpCheckpointStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mTmpCheckpointStream.close();

    // Delete the temporary checkpoint if there is a newer checkpoint committed.
    UfsJournalFile checkpoint = UfsJournalSnapshot.getSnapshot(mJournal).getLatestCheckpoint();
    if (checkpoint != null) {
      if (mCheckpointFile.getEnd() <= checkpoint.getEnd()) {
        mUfs.deleteFile(mTmpCheckpointFileLocation.toString());
        return;
      }
    }

    try {
      mUfs.mkdirs(mJournal.getCheckpointDir().toString());
    } catch (IOException e) {
      if (!mUfs.exists(mJournal.getCheckpointDir().toString())) {
        LOG.warn("Failed to create the checkpoint directory {}.", mJournal.getCheckpointDir());
        throw e;
      }
    }
    String dst = mCheckpointFile.getLocation().toString();
    try {
      if (!mUfs.renameFile(mTmpCheckpointFileLocation.toString(), dst)) {
        throw new IOException(String
            .format("Failed to rename %s to %s.", mTmpCheckpointFileLocation.toString(), dst));
      }
    } catch (IOException e) {
      if (!mUfs.exists(dst)) {
        LOG.warn("Failed to commit checkpoint from {} to {} with error {}.",
            mTmpCheckpointFileLocation, dst, e.getMessage());
      }
      try {
        mUfs.deleteFile(mTmpCheckpointFileLocation.toString());
      } catch (IOException ee) {
        LOG.warn("Failed to clean up temporary checkpoint {} at entry {}.",
            mTmpCheckpointFileLocation, mCheckpointFile.getEnd());
      }
      throw e;
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    mTmpCheckpointStream.close();
    if (mUfs.exists(mTmpCheckpointFileLocation.toString())) {
      mUfs.deleteFile(mTmpCheckpointFileLocation.toString());
    }
  }
}
