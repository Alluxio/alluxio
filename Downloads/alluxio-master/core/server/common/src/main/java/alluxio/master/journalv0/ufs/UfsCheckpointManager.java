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

package alluxio.master.journalv0.ufs;

import alluxio.underfs.UnderFileSystem;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Manages the checkpoint for a journal. The {@link #update(URI)} method will update the
 * journal's checkpoint to the specified location, and {@link #recover()} will recover from any
 * failures that may occur during {@link #update(URI)}.
 *
 * The checkpoint updating process goes
 * <pre>
 * 1. Write a new checkpoint named checkpoint.data.tmp
 * 2. Rename checkpoint.data to checkpoint.data.backup.tmp
 * 3. Rename checkpoint.data.backup.tmp to checkpoint.data.backup
 * 4. Rename checkpoint.data.tmp to checkpoint.data
 * 5. Delete completed logs
 * 6. Delete checkpoint.data.backup
 * </pre>
 */
public final class UfsCheckpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(UfsCheckpointManager.class);

  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;
  /**
   * The checkpoint location. This is where the latest checkpoint is stored. During
   * normal operation (when not writing a new checkpoint), this is the only checkpoint
   * that exists. If the location exists and there is no {@link #mTempBackupCheckpoint}, it plus
   * all completed logs, plus the current log, should represent the full state of the master.
   */
  private final URI mCheckpoint;
  /**
   * The backup checkpoint location. The latest checkpoint is saved here while renaming
   * the temporary checkpoint so that we can recover in case the rename fails. If this location and
   * {@link #mCheckpoint} both exist, {@link #mCheckpoint} is the most up to date checkpoint
   * and the backup checkpoint should be deleted.
   */
  private final URI mBackupCheckpoint;
  /**
   * The temporary backup checkpoint location. This location is used as an intermediate
   * rename step when backing up {@link #mCheckpoint} to {@link #mBackupCheckpoint}. As long
   * as this location exists, it supersedes {@link #mCheckpoint} as the most up to date
   * checkpoint file.
   */
  private final URI mTempBackupCheckpoint;
  /**
   * A journal writer through which this checkpoint manager can delete completed logs when the
   * checkpoint is updated.
   */
  private final UfsJournalWriter mWriter;

  /**
   * Creates a new instance of {@link UfsCheckpointManager}.
   *
   * @param ufs the under file system holding the journal
   * @param checkpoint the location of the checkpoint
   * @param writer a journal writer which can be used to delete completed logs
   */
  public UfsCheckpointManager(UnderFileSystem ufs, URI checkpoint, UfsJournalWriter writer) {
    mUfs = ufs;
    mCheckpoint = checkpoint;
    try {
      mBackupCheckpoint = new URI(mCheckpoint + ".backup");
      mTempBackupCheckpoint = new URI(mBackupCheckpoint + ".tmp");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    mWriter = writer;
  }

  /**
   * Recovers the checkpoint in case the master crashed while updating it.
   */
  public void recover() {
    try {
      boolean checkpointExists = mUfs.isFile(mCheckpoint.toString());
      boolean backupCheckpointExists = mUfs.isFile(mBackupCheckpoint.toString());
      boolean tempBackupCheckpointExists = mUfs.isFile(mTempBackupCheckpoint.toString());
      Preconditions
          .checkState(!(checkpointExists && backupCheckpointExists && tempBackupCheckpointExists),
              "checkpoint, temp backup checkpoint, and backup checkpoint should never all exist ");
      if (tempBackupCheckpointExists) {
        // If mCheckpointPath also exists, step 2 must have implemented rename as copy + delete, and
        // failed during the delete.
        UnderFileSystemUtils.deleteFileIfExists(mUfs, mCheckpoint.toString());
        mUfs.renameFile(mTempBackupCheckpoint.toString(), mCheckpoint.toString());
      }
      if (backupCheckpointExists) {
        // We must have crashed after step 3
        if (checkpointExists) {
          // We crashed after step 4, so we can finish steps 5 and 6.
          mWriter.deleteCompletedLogs();
          mUfs.deleteFile(mBackupCheckpoint.toString());
        } else {
          // We crashed before step 4, so we roll back to the backup checkpoint.
          mUfs.renameFile(mBackupCheckpoint.toString(), mCheckpoint.toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Updates the checkpoint to the specified URI.
   *
   * @param location the location of the new checkpoint
   */
  public void update(URI location) {
    try {
      if (mUfs.isFile(mCheckpoint.toString())) {
        UnderFileSystemUtils.deleteFileIfExists(mUfs, mTempBackupCheckpoint.toString());
        UnderFileSystemUtils.deleteFileIfExists(mUfs, mBackupCheckpoint.toString());
        // Rename in two steps so that we never have identical mCheckpointPath and
        // mBackupCheckpointPath. This is a concern since UFS may implement rename as copy + delete.
        mUfs.renameFile(mCheckpoint.toString(), mTempBackupCheckpoint.toString());
        mUfs.renameFile(mTempBackupCheckpoint.toString(), mBackupCheckpoint.toString());
        LOG.info("Backed up the checkpoint file to {}", mBackupCheckpoint.toString());
      }
      mUfs.renameFile(location.getPath(), mCheckpoint.toString());
      LOG.info("Renamed the checkpoint file from {} to {}", location,
          mCheckpoint.toString());

      // The checkpoint already reflects the information in the completed logs.
      mWriter.deleteCompletedLogs();
      UnderFileSystemUtils.deleteFileIfExists(mUfs, mBackupCheckpoint.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
