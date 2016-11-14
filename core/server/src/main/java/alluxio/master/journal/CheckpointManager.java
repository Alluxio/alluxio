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

package alluxio.master.journal;

import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A class which manages the checkpoint for a journal. The {@link #updateCheckpoint(String)} method
 * will update the journal's checkpoint file to a specified checkpoint, and
 * {@link recoverCheckpoint()} will recover from any failures that may occur during
 * {@link #updateCheckpoint(String)}.
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
public final class CheckpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;
  /**
   * Absolute path to the checkpoint file. This is where the latest checkpoint is stored. During
   * normal operation (when not writing a new checkpoint file), this is the only checkpoint file
   * that exists. If this file exists and there is no {@link #mTempBackupCheckpointPath}, it plus
   * all completed logs, plus the active log, should represent the full state of the master.
   */
  private final String mCheckpointPath;
  /**
   * Absolute path to the backup checkpoint file. The latest checkpoint is saved here while renaming
   * the temporary checkpoint so that we can recover in case the rename fails. If this file and
   * {@link #mCheckpointPath} both exist, {@link #mCheckpointPath} is the most up to date checkpoint
   * and {@link #mBackupCheckpointPath} should be deleted.
   */
  private final String mBackupCheckpointPath;
  /**
   * Absolute path to the temporary backup checkpoint file. This path is used as an intermediate
   * rename step when backing up {@link #mCheckpointPath} to {@link #mBackupCheckpointPath}. As long
   * as this file exists, it supercedes mCheckpointPath as the most up to date checkpoint file.
   */
  private final String mTempBackupCheckpointPath;
  /**
   * A journal writer through which this checkpoint manager can delete completed logs when the
   * checkpoint is updated.
   */
  private final JournalWriter mWriter;

  /**
   * Creates a new instance of {@link CheckpointManager}.
   *
   * @param ufs the under file system holding the journal
   * @param checkpointPath the path to the checkpoint file
   * @param writer a journal writer which can be used to delete completed logs
   */
  public CheckpointManager(UnderFileSystem ufs, String checkpointPath, JournalWriter writer) {
    mUfs = ufs;
    mCheckpointPath = checkpointPath;
    mBackupCheckpointPath = mCheckpointPath + ".backup";
    mTempBackupCheckpointPath = mBackupCheckpointPath + ".tmp";
    mWriter = writer;
  }

  /**
   * Recovers the checkpoint file in case the master crashed while updating it previously.
   *
   * After this method has completed, the checkpoint at {@link #mCheckpointPath} plus any completed
   * logs will fully represent the master's state, and there will be no files at
   * {@link mBackupCheckpointPath} or {@link #mTempBackupCheckpointPath}.
   */
  public void recoverCheckpoint() {
    try {
      boolean checkpointExists = mUfs.isFile(mCheckpointPath);
      boolean backupCheckpointExists = mUfs.isFile(mBackupCheckpointPath);
      boolean tempBackupCheckpointExists = mUfs.isFile(mTempBackupCheckpointPath);
      Preconditions.checkState(
          !(checkpointExists && backupCheckpointExists && tempBackupCheckpointExists),
          "checkpoint, temp backup checkpoint, and backup checkpoint should never all exist ");
      if (tempBackupCheckpointExists) {
        // If mCheckpointPath also exists, step 2 must have implemented rename as copy + delete, and
        // failed during the delete.
        UnderFileSystemUtils.deleteFileIfExists(mCheckpointPath);
        mUfs.renameFile(mTempBackupCheckpointPath, mCheckpointPath);
      }
      if (backupCheckpointExists) {
        // We must have crashed after step 3
        if (checkpointExists) {
          // We crashed after step 4, so we can finish steps 5 and 6.
          mWriter.deleteCompletedLogs();
          mUfs.delete(mBackupCheckpointPath, false);
        } else {
          // We crashed before step 4, so we roll back to the backup checkpoint.
          mUfs.renameFile(mBackupCheckpointPath, mCheckpointPath);
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Updates the checkpoint file to the checkpoint at the specified path. The update is done in such
   * a way that {@link #recoverCheckpoint()} will recover from any failures that occur during the
   * update.
   *
   * @param newCheckpointPath the path to the new checkpoint file
   */
  public void updateCheckpoint(String newCheckpointPath) {
    try {
      if (mUfs.isFile(mCheckpointPath)) {
        UnderFileSystemUtils.deleteFileIfExists(mTempBackupCheckpointPath);
        UnderFileSystemUtils.deleteFileIfExists(mBackupCheckpointPath);
        // Rename in two steps so that we never have identical mCheckpointPath and
        // mBackupCheckpointPath. This is a concern since UFS may implement rename as copy + delete.
        mUfs.renameFile(mCheckpointPath, mTempBackupCheckpointPath);
        mUfs.renameFile(mTempBackupCheckpointPath, mBackupCheckpointPath);
        LOG.info("Backed up the checkpoint file to {}", mBackupCheckpointPath);
      }
      mUfs.renameFile(newCheckpointPath, mCheckpointPath);
      LOG.info("Renamed the checkpoint file from {} to {}", newCheckpointPath, mCheckpointPath);

      // The checkpoint already reflects the information in the completed logs.
      mWriter.deleteCompletedLogs();
      UnderFileSystemUtils.deleteFileIfExists(mBackupCheckpointPath);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
