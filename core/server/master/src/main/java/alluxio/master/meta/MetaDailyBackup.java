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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.URIUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BackupOptions;
import alluxio.wire.BackupResponse;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Backing up primary master metadata everyday at a fixed UTC time.
 */
public final class MetaDailyBackup {
  private static final Logger LOG = LoggerFactory.getLogger(MetaDailyBackup.class);

  public static final String BACKUP_FILE_FORMAT = "alluxio-backup-%s-%s.gz";
  public static final Pattern BACKUP_FILE_PATTERN
      = Pattern.compile("alluxio-backup-[0-9]+-[0-9]+-[0-9]+-([0-9]+).gz");

  private final boolean mIsLocal;
  private final int mMaxFile;
  private final MetaMaster mMetaMaster;
  private final UnderFileSystem mUnderfileSystem;

  private ScheduledFuture<?> mBackup;
  private String mBackupDir;
  private ScheduledExecutorService mScheduleExecutor;

  /**
   * Constructs a new {@link MetaDailyBackup}.
   *
   * @param metaMaster the meta master
   */
  MetaDailyBackup(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
    mMaxFile = Configuration.getInt(PropertyKey.MASTER_DAILY_BACKUP_FILES_RETAINED);
    mBackupDir = Configuration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);

    if (URIUtils.isLocalFilesystem(Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))) {
      mIsLocal = true;
      mUnderfileSystem = UnderFileSystem.Factory
          .create("/", UnderFileSystemConfiguration.defaults());
    } else {
      mIsLocal = false;
      mUnderfileSystem = UnderFileSystem.Factory.createForRoot();
    }
  }

  /**
   * Starts {@link MetaDailyBackup}.
   */
  public void start() {
    Preconditions.checkState(mBackup == null && mScheduleExecutor == null);

    mScheduleExecutor = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("MetaDailyBackup-%d", true));

    long delayedTimeInMillis = getTimeToNextBackup();
    mBackup = mScheduleExecutor.scheduleAtFixedRate(this::dailyBackup,
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
    LocalTime backupTime = LocalTime.parse(Configuration
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
      BackupResponse resp = mMetaMaster.backup(new BackupOptions(mBackupDir, mIsLocal));
      if (mIsLocal) {
        LOG.info("Successfully backed up journal to {} on master {}",
            resp.getBackupUri(), resp.getHostname());
      } else {
        LOG.info("Successfully backed up journal to {}", resp.getBackupUri());
      }
      try {
        deleteOldestBackups();
      } catch (Throwable t) {
        LOG.error("Failed to delete outdated backup files at {}", mBackupDir, t);
      }
    } catch (Throwable t) {
      LOG.error("Failed to execute daily backup at {}", mBackupDir, t);
    }
  }

  /**
   * Deletes stale backup files to avoid consuming too many spaces.
   */
  private void deleteOldestBackups() throws Exception {
    UfsStatus[] statuses = mUnderfileSystem.listStatus(mBackupDir);
    if (statuses.length <= mMaxFile) {
      return;
    }

    // Sort the backup files according to create time from oldest to newest
    TreeMap<Instant, String> timeToFile = new TreeMap<>((a, b) -> (
        a.isBefore(b) ? -1 : a.isAfter(b) ? 1 : 0));
    for (UfsStatus status : statuses) {
      if (status.isFile()) {
        Matcher matcher = BACKUP_FILE_PATTERN.matcher(status.getName());
        if (matcher.matches()) {
          timeToFile.put(Instant.ofEpochMilli(Long.parseLong(matcher.group(1))),
              status.getName());
        }
      }
    }

    // Delete the oldest files
    int toDeleteFileNum = timeToFile.size() - mMaxFile;
    if (toDeleteFileNum <= 0) {
      return;
    }
    for (int i = 0; i < toDeleteFileNum; i++) {
      String toDeleteFile = PathUtils.concatPath(mBackupDir,
          timeToFile.pollFirstEntry().getValue());
      mUnderfileSystem.deleteFile(toDeleteFile);
    }
    LOG.info("Deleted {} outdated metadata backup files at {}", toDeleteFileNum, mBackupDir);
  }

  /**
   * Stops {@link MetaDailyBackup}.
   */
  public void stop() {
    if (mBackup != null) {
      mBackup.cancel(true);
      mBackup = null;
    }
    mScheduleExecutor.shutdown();
    LOG.info("Daily metadata backup stopped.");
  }
}
