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
import alluxio.util.FormatUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BackupOptions;
import alluxio.wire.BackupResponse;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
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

  private final Pattern mBackupPattern
      = Pattern.compile("alluxio-backup-([0-9]+)-([0-9]+)-([0-9]+)-[^.]+.gz");

  private ScheduledFuture<?> mBackup;
  private MetaMaster mMetaMaster;
  private ScheduledExecutorService mScheduleExecutor;

  /**
   * Constructs a new {@link MetaDailyBackup}.
   *
   * @param metaMaster the meta master
   */
  MetaDailyBackup(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  /**
   * Starts {@link MetaDailyBackup}.
   */
  public void start() {
    Preconditions.checkState(mBackup == null && mScheduleExecutor == null);
    mScheduleExecutor = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("MetaDailyBackup-%d", true));

    mBackup = mScheduleExecutor.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            dailyBackup();
          }
        }, getTimeToNextBackup(), FormatUtils.parseTimeSize("1day"),
        TimeUnit.MILLISECONDS);
  }

  /**
   * Gets the time gap between now and next backup time.
   *
   * @return the time gap to next backup
   */
  private long getTimeToNextBackup() {
    LocalDateTime now = LocalDateTime.now(Clock.systemUTC());
    String[] hourAndMin = Configuration.get(PropertyKey.MASTER_DAILY_BACKUP_TIME).split(":");
    int hour = Integer.parseInt(hourAndMin[0]);
    int min = hourAndMin.length == 2 ? Integer.parseInt(hourAndMin[1]) : 0;
    LocalDateTime nextBackupTime = LocalDateTime.of(now.getYear(),
        now.getMonth(), now.getDayOfMonth(), hour, min, 0);
    if (nextBackupTime.isBefore(now)) {
      nextBackupTime = nextBackupTime.plusDays(1);
    }
    return ChronoUnit.MILLIS.between(now, nextBackupTime);
  }

  /**
   * The daily backup task.
   */
  private void dailyBackup() {
    String dir = Configuration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);
    try {
      BackupResponse resp = mMetaMaster.backup(new BackupOptions(dir, false));
      LOG.info("Successfully backed up journal to %s%n", resp.getBackupUri());
    } catch (Exception e) {
      LOG.error("Failed to execute daily backup at {}", dir, e);
    }
    try {
      deleteOldestBackups(dir);
    } catch (Exception e) {
      LOG.error("Failed to delete outdated backup files", e);
    }
  }

  /**
   * Deletes oldest backup files to avoid consuming too many spaces.
   */
  private void deleteOldestBackups(String dir) throws Exception {
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot();
    UfsStatus[] statues = ufs.listStatus(dir);
    if (statues.length < 4) {
      return;
    }
    TreeMap<LocalDate, String> map = new TreeMap<>((a, b) -> (
        a.isBefore(b) ? 1 : a.isAfter(b) ? -1 : 0));
    for (UfsStatus status : statues) {
      if (status.isFile()) {
        Matcher matcher = mBackupPattern.matcher(status.getName());
        if (matcher.matches()) {
          LocalDate date = LocalDate.of(Integer.parseInt(matcher.group(0)),
              Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
          map.put(date, status.getName());
        }
      }
    }
    if (map.size() < 3) {
      return;
    }
    for (int i = 0; i < map.size() - 3; i++) {
      ufs.deleteFile(PathUtils.concatPath(dir, map.pollFirstEntry().getValue()));
    }
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
  }
}
