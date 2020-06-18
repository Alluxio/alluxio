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
import alluxio.exception.BackupException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.BackupState;
import alluxio.resource.LockResource;
import alluxio.wire.BackupStatus;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Used to track and update status of a backup.
 */
public class BackupTracker {
  private static final Logger LOG = LoggerFactory.getLogger(BackupTracker.class);

  /** Underlying backup status. */
  private BackupStatus mBackupStatus;
  /** Settable future for tracking the completion of the backup. */
  private SettableFuture<Void> mCompletion;
  /** Used to provide counter to backup facility. */
  private AtomicLong mEntryCounter;

  /** Used to replace current backup status safely. */
  private Lock mStatusLock = new ReentrantLock(true);

  /** Stores statuses for finished backups. */
  private Map<UUID, BackupStatus> mFinishedBackups = new ConcurrentHashMap<>();

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
    LOG.info("Resetting backup tracker.");
    try (LockResource statusLock = new LockResource(mStatusLock)) {
      // Set error for backup in-progress.
      if (inProgress()) {
        LOG.info("Resetting the pending backup.");
        updateError(new BackupException("Backup reset by tracker"));
      }
      // Reset current backup status.
      mBackupStatus = new BackupStatus(BackupState.None);
      mEntryCounter = new AtomicLong(0);
      mCompletion = SettableFuture.create();
    }
  }

  /**
   * @return the status of the current backup
   */
  public BackupStatus getCurrentStatus() {
    try (LockResource statusLock = new LockResource(mStatusLock)) {
      return new BackupStatus(mBackupStatus).setEntryCount(mEntryCounter.get());
    }
  }

  /**
   * @param backupId the backup id
   * @return the status of a backup
   */
  public BackupStatus getStatus(UUID backupId) {
    // Return the current backup status if backup-id matches.
    BackupStatus currentStatus = getCurrentStatus();
    if (currentStatus.getBackupId().equals(backupId)) {
      return currentStatus;
    }
    // Try to return result from finished backups. Return default if none exists.
    return mFinishedBackups.getOrDefault(backupId, new BackupStatus(backupId, BackupState.None));
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
    Preconditions.checkNotNull(error);
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
      AlluxioException e = mBackupStatus.getError();
      throw (e != null) ? e : new BackupException("unknown error");
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
   * @return {@code true} if a backup is in progress
   */
  public boolean inProgress() {
    return mBackupStatus != null && mBackupStatus.getState() != BackupState.None
        && mCompletion != null && !mCompletion.isDone();
  }

  /**
   * Used to signal finished backup when completed or failed.
   */
  private void signalIfFinished() {
    // Store status if finished.
    if (mBackupStatus.isFinished()) {
      mFinishedBackups.put(mBackupStatus.getBackupId(),
          mBackupStatus.setEntryCount(mEntryCounter.get()));
    }

    if (mBackupStatus.isCompleted()) {
      mCompletion.set(null);
    } else if (mBackupStatus.isFailed()) {
      mCompletion.setException(mBackupStatus.getError());
    }
  }
}
