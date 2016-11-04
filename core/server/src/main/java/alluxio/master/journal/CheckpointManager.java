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
 *
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
   * @param the directory for the journal
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
   * The checkpointing process goes
   * <pre>
   * 1. Write mTempCheckpointPath based on all completed logs
   * 2. Rename mCheckpointPath to mTempBackupCheckpointPath
   * 3. Rename mTempBackupCheckpointPath to mBackupCheckpointPath
   * 4. Rename mTempCheckpointPath to mCheckpointPath
   * 5. Delete completed logs
   * 6. Delete mBackupCheckpointPath
   * </pre>
   */
  public void recoverCheckpoint() {
    try {
      Preconditions.checkState(
          !(mUfs.exists(mCheckpointPath) && mUfs.exists(mTempBackupCheckpointPath)
              && mUfs.exists(mBackupCheckpointPath)),
          "checkpoint, temp backup checkpoint, and backup checkpoint should never exist "
              + "simultaneously");
      if (mUfs.exists(mTempBackupCheckpointPath)) {
        // If mCheckpointPath exists, step 2 must have implemented rename as copy + delete, and
        // failed during the delete.
        UnderFileSystemUtils.deleteIfExists(mUfs, mCheckpointPath);
        mUfs.rename(mTempBackupCheckpointPath, mCheckpointPath);
      }
      if (mUfs.exists(mBackupCheckpointPath)) {
        // We must have crashed after step 3
        if (mUfs.exists(mCheckpointPath)) {
          // We crashed after step 4, so we can finish steps 5 and 6.
          mWriter.deleteCompletedLogs();
          mUfs.delete(mBackupCheckpointPath, false);
        } else {
          // We crashed before step 4, so we roll back to backup checkpoint.
          mUfs.rename(mBackupCheckpointPath, mCheckpointPath);
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @throws Exception
   *
   */
  public void updateCheckpoint(String newCheckpointPath) {
    try {
      if (mUfs.exists(mCheckpointPath)) {
        UnderFileSystemUtils.deleteIfExists(mUfs, mTempBackupCheckpointPath);
        UnderFileSystemUtils.deleteIfExists(mUfs, mBackupCheckpointPath);
        // Rename in two steps so that we never have identical mCheckpointPath and
        // mBackupCheckpointPath. This is a concern since UFS may implement rename as copy + delete.
        mUfs.rename(mCheckpointPath, mTempBackupCheckpointPath);
        mUfs.rename(mTempBackupCheckpointPath, mBackupCheckpointPath);
      }
      mUfs.rename(newCheckpointPath, mCheckpointPath);
      LOG.info("Renamed checkpoint file {} to {}", newCheckpointPath, mCheckpointPath);

      // The checkpoint already reflects the information in the completed logs.
      mWriter.deleteCompletedLogs();
      UnderFileSystemUtils.deleteIfExists(mUfs, mBackupCheckpointPath);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
