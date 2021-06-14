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

import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Stream for writing checkpoints to the UFS. The secondary masters use this to periodically create
 * new checkpoints.
 *
 * It first writes the checkpoint to a temporary location. After it is done with writing the
 * temporary checkpoint, it commits the checkpoint by renaming the temporary checkpoint to the final
 * location. If the same checkpoint has already been created by another secondary master, the
 * checkpoint is aborted.
 */
@NotThreadSafe
final class UfsJournalCheckpointWriter extends FilterOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalCheckpointWriter.class);

  private final UfsJournal mJournal;
  private final UnderFileSystem mUfs;

  /** The checkpoint file to be committed to. */
  private final UfsJournalFile mCheckpointFile;
  /** The location for the temporary checkpoint. */
  private final URI mTmpCheckpointFileLocation;

  /** Whether this journal writer is closed. */
  private boolean mClosed;

  /**
   * Creates a new instance of {@link UfsJournalCheckpointWriter}.
   *
   * @param journal the handle to the journal
   * @param tmpPath the temporary path to initially write the checkpoint to
   * @param checkpointFile the file to copy the checkpoint to on success
   */
  private UfsJournalCheckpointWriter(UfsJournal journal, URI tmpPath, UfsJournalFile checkpointFile)
      throws IOException {
    super(journal.getUfs().create(tmpPath.toString()));
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = mJournal.getUfs();

    mTmpCheckpointFileLocation = tmpPath;
    mCheckpointFile = checkpointFile;
  }

  public static UfsJournalCheckpointWriter create(UfsJournal journal, long snapshotSequenceNumber)
      throws IOException {
    URI tmpPath = UfsJournalFile.encodeTemporaryCheckpointFileLocation(journal);
    URI finalPath = UfsJournalFile.encodeCheckpointFileLocation(journal, snapshotSequenceNumber);
    UfsJournalFile checkpointFile =
        UfsJournalFile.createCheckpointFile(finalPath, snapshotSequenceNumber);
    return new UfsJournalCheckpointWriter(journal, tmpPath, checkpointFile);
  }

  @Override
  public void write(byte[] b, int offset, int length) throws IOException {
    out.write(b, offset, length);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    out.close();

    // Delete the temporary checkpoint if there is a newer checkpoint committed.
    UfsJournalFile latestCheckpoint =
        UfsJournalSnapshot.getSnapshot(mJournal).getLatestCheckpoint();
    if (latestCheckpoint != null) {
      if (mCheckpointFile.getEnd() <= latestCheckpoint.getEnd()) {
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
            mTmpCheckpointFileLocation, dst, e.toString());
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

  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    out.close();
    if (mUfs.exists(mTmpCheckpointFileLocation.toString())) {
      mUfs.deleteFile(mTmpCheckpointFileLocation.toString());
    }
  }
}
