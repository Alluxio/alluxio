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

package alluxio.master.backup;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.BackupState;
import alluxio.wire.BackupStatus;

import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used to track and update status of a backup.
 */
public class BackupTracker {
  /** Underlying backup status. */
  private BackupStatus mBackupStatus;
  /** Settable future for tracking the completion of the backup. */
  private SettableFuture<Void> mCompletion;
  /** Used to provide counter to backup facility. */
  private AtomicLong mEntryCounter;

  /**
   * Creates a tracker.
   */
  public BackupTracker() {
    reset();
  }

  /**
   * Resets this tracker.
   */
  public void reset() {
    mBackupStatus = new BackupStatus(BackupState.None);
    mEntryCounter = new AtomicLong(0);
    // Fail potentials waiters before resetting the future.
    if (mCompletion != null && !mCompletion.isDone()) {
      mCompletion.setException(new RuntimeException("Tracker reset"));
    }
    mCompletion = SettableFuture.create();
  }

  /**
   * @return the current status of a backup
   */
  public BackupStatus getCurrentStatus() {
    return new BackupStatus(mBackupStatus).setEntryCount(mEntryCounter.get());
  }

  /**
   * @return the entry counter
   */
  public AtomicLong getEntryCounter() {
    return mEntryCounter;
  }

  /**
   * Replaces the internal status with given status.
   *
   * @param status the backup status
   */
  public void update(BackupStatus status) {
    mBackupStatus = status;
    mEntryCounter.set(status.getEntryCount());
    signalIfFinished();
  }

  /**
   * Updates hostname of backup status.
   *
   * @param hostname the hostname
   */
  public void updateHostname(String hostname) {
    mBackupStatus.setHostname(hostname);
  }

  /**
   * Updates backup URI of status.
   *
   * @param backupUri the backup URI
   */
  public void updateBackupUri(AlluxioURI backupUri) {
    mBackupStatus.setBackupUri(backupUri);
  }

  /**
   * Updates the state of backup status.
   *
   * @param state the backup state
   */
  public void updateState(BackupState state) {
    mBackupStatus.setState(state);
    signalIfFinished();
  }

  /**
   * Updates the error of backup status.
   *
   * @param error the backup error
   */
  public void updateError(AlluxioException error) {
    mBackupStatus.setError(error);
    signalIfFinished();
  }

  /**
   * Used to wait until this backup is finished.
   *
   * @throws AlluxioStatusException if backup failed
   */
  public void waitUntilFinished() throws AlluxioException {
    try {
      if (!mBackupStatus.isFinished()) {
        mCompletion.get();
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while waiting for backup to finish.");
    } catch (ExecutionException ee) {
      // mCompletion is only failed with status error.
      throw mBackupStatus.getError();
    }
  }

  /**
   * Used to wait until this backup is finished.
   *
   * @param timeout timeout duration
   * @param unit time unit
   * @return {@code true} if backup is finished
   */
  public boolean waitUntilFinished(long timeout, TimeUnit unit) {
    try {
      if (!mBackupStatus.isFinished()) {
        mCompletion.get(timeout, unit);
        return true;
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while waiting for backup to finish.");
    } catch (ExecutionException ee) {
      // Finished with failure.
      return true;
    } catch (TimeoutException te) {
      // Couldn't see completion.
    }
    return mBackupStatus.isFinished();
  }

  /**
   *
   * @return {@code true} if backup is finished. (completed | failed)
   */
  public boolean isFinished() {
    return mBackupStatus.isFinished();
  }

  /**
   * @return {@code true} if a backup is in progress
   */
  public boolean inProgress() {
    return mBackupStatus.getState() != BackupState.None && !isFinished();
  }

  /**
   * Used to signal finished backup when completed or failed.
   */
  private void signalIfFinished() {
    if (mBackupStatus.isCompleted()) {
      mCompletion.set(null);
    } else if (mBackupStatus.isFailed()) {
      mCompletion.setException(mBackupStatus.getError());
    }
  }
}
