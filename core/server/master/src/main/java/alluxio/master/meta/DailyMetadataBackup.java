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

package alluxio.master.meta;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPRequest;
import alluxio.master.BackupManager;
import alluxio.master.StateLockOptions;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BackupStatus;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 * Backing up primary master metadata everyday at a fixed UTC time.
 */
public final class DailyMetadataBackup {
  private static final Logger LOG = LoggerFactory.getLogger(DailyMetadataBackup.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 5L * Constants.SECOND_MS;

  private final String mBackupDir;
  private final boolean mIsLocal;
  private final MetaMaster mMetaMaster;
  private final int mRetainedFiles;
  private final ScheduledExecutorService mScheduledExecutor;
  private final UfsManager mUfsManager;

  private ScheduledFuture<?> mBackup;

  /**
   * Constructs a new {@link DailyMetadataBackup}.
   *
   * @param metaMaster the meta master
   * @param service a scheduled executor service
   * @param ufsManager the under file system Manager
   */
  DailyMetadataBackup(MetaMaster metaMaster,
      ScheduledExecutorService service, UfsManager ufsManager) {
    mMetaMaster = metaMaster;
    mBackupDir = ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);
    mRetainedFiles = ServerConfiguration.getInt(PropertyKey.MASTER_DAILY_BACKUP_FILES_RETAINED);
    mScheduledExecutor = service;
    mUfsManager = ufsManager;
    try (CloseableResource<UnderFileSystem> ufsResource =
             mUfsManager.getRoot().acquireUfsResource()) {
      mIsLocal = ufsResource.get().getUnderFSType().equals("local");
    }
  }

  /**
   * Starts {@link DailyMetadataBackup}.
   */
  public void start() {
    Preconditions.checkState(mBackup == null);
    long delayedTimeInMillis = getTimeToNextBackup();
    mBackup = mScheduledExecutor.scheduleAtFixedRate(this::dailyBackup,
        delayedTimeInMillis, FormatUtils.parseTimeSize("1day"), TimeUnit.MILLISECONDS);
    LOG.info("Daily metadata backup scheduled to start in {}",
        CommonUtils.convertMsToClockTime(delayedTimeInMillis));
  }

  /**
   * Gets the time gap between now and next backup time.
   *
   * @return the time gap to next backup
   */
  private long getTimeToNextBackup() {
    LocalDateTime now = LocalDateTime.now(Clock.systemUTC());

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("H:mm");
    LocalTime backupTime = LocalTime.parse(ServerConfiguration
        .get(PropertyKey.MASTER_DAILY_BACKUP_TIME), formatter);
    LocalDateTime nextBackupTime = now.withHour(backupTime.getHour())
        .withMinute(backupTime.getMinute());
    if (nextBackupTime.isBefore(now)) {
      nextBackupTime = nextBackupTime.plusDays(1);
    }
    return ChronoUnit.MILLIS.between(now, nextBackupTime);
  }

  /**
   * The daily backup task.
   */
  private void dailyBackup() {
    try {
      BackupStatus resp = mMetaMaster.backup(
          BackupPRequest.newBuilder().setTargetDirectory(mBackupDir)
              .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(mIsLocal)).build(),
          StateLockOptions.defaultsForDailyBackup());
      if (mIsLocal) {
        LOG.info("Successfully backed up journal to {} on master {} with {} entries.",
            resp.getBackupUri(), resp.getHostname(), resp.getEntryCount());
      } else {
        LOG.info("Successfully backed up journal to {} with {} entries.",
            resp.getBackupUri(), resp.getEntryCount());
      }
    } catch (Throwable t) {
      LOG.error("Failed to execute daily backup at {}", mBackupDir, t);
      return;
    }

    try {
      deleteStaleBackups();
    } catch (Throwable t) {
      LOG.error("Failed to delete outdated backup files at {}", mBackupDir, t);
    }
  }

  /**
   * Deletes stale backup files to avoid consuming too many spaces.
   */
  private void deleteStaleBackups() throws Exception {
    try (CloseableResource<UnderFileSystem> ufsResource =
             mUfsManager.getRoot().acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      UfsStatus[] statuses = ufs.listStatus(mBackupDir);
      if (statuses.length <= mRetainedFiles) {
        return;
      }

      // Sort the backup files according to create time from oldest to newest
      TreeMap<Instant, String> timeToFile = new TreeMap<>((a, b) -> (
          a.isBefore(b) ? -1 : a.isAfter(b) ? 1 : 0));
      for (UfsStatus status : statuses) {
        if (status.isFile()) {
          Matcher matcher = BackupManager.BACKUP_FILE_PATTERN.matcher(status.getName());
          if (matcher.matches()) {
            timeToFile.put(Instant.ofEpochMilli(Long.parseLong(matcher.group(1))),
                status.getName());
          }
        }
      }

      int toDeleteFileNum = timeToFile.size() - mRetainedFiles;
      if (toDeleteFileNum <= 0) {
        return;
      }
      for (int i = 0; i < toDeleteFileNum; i++) {
        String toDeleteFile = PathUtils.concatPath(mBackupDir,
            timeToFile.pollFirstEntry().getValue());
        ufs.deleteExistingFile(toDeleteFile);
      }
      LOG.info("Deleted {} stale metadata backup files at {}", toDeleteFileNum, mBackupDir);
    }
  }

  /**
   * Stops {@link DailyMetadataBackup}.
   */
  public void stop() {
    if (mBackup != null) {
      mBackup.cancel(true);
      mBackup = null;
    }
    LOG.info("Daily metadata backup stopped");

    mScheduledExecutor.shutdownNow();
    String waitForMessage = "waiting for daily metadata backup executor service to shut down";
    try {
      if (!mScheduledExecutor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        LOG.warn("Timed out " + waitForMessage);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while " + waitForMessage);
    }
  }
}
