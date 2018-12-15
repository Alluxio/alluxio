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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.master.BackupManager;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ControllableScheduler;
import alluxio.util.io.PathUtils;
import alluxio.wire.BackupResponse;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests the {@link MetaDailyBackup}.
 */
public class MetaDailyBackupTest {
  private MetaMaster mMetaMaster;
  private ControllableScheduler mScheduler;
  private UnderFileSystem mUfs;
  private Random mRandom;

  @Test
  public void test() throws Exception {
    try (Closeable c =
        new ConfigurationRule(ImmutableMap.of(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, "true",
        PropertyKey.MASTER_DAILY_BACKUP_FILES_RETAINED, "1")).toResource()) {
      MetaDailyBackup dailyBackup = new MetaDailyBackup(mMetaMaster, mScheduler, mUfs);
      dailyBackup.start();

      // Trigger the runnable within 3 days
      mScheduler.jumpToFuture(3, TimeUnit.DAYS);

      verify(mMetaMaster, times(3)).backup(any());
      verify(mUfs, times(3)).listStatus(any());
      verify(mUfs, times(6)).deleteFile(any());
    }
  }

  @Before
  public void before() throws Exception {
    mRandom = new Random();
    String backupDir = Configuration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);

    mMetaMaster = Mockito.mock(MetaMaster.class);
    when(mMetaMaster.backup(any())).thenReturn(new BackupResponse(new AlluxioURI(
        PathUtils.concatPath(backupDir, generateBackupFileName())), "localhost"));

    mUfs = Mockito.mock(UnderFileSystem.class);
    when(mUfs.getUnderFSType()).thenReturn("local");
    when(mUfs.listStatus(backupDir)).thenReturn(getUfsStatuses());
    when(mUfs.deleteFile(any())).thenReturn(true);

    mScheduler = new ControllableScheduler();
  }

  /**
   * @return the random generated ufs file statuses
   */
  private UfsStatus[] getUfsStatuses() {
    Random random = new Random();
    UfsStatus[] statuses = new UfsFileStatus[3];
    for (int i = 0; i < statuses.length; i++) {
      statuses[i] = new UfsFileStatus(generateBackupFileName(),
          CommonUtils.randomAlphaNumString(10), random.nextLong(), random.nextLong(),
          CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10),
          (short) random.nextInt());
    }
    return statuses;
  }

  /**
   * Generates a backup file name used a time that includes some randomness.
   *
   * @return a backup file name
   */
  private String generateBackupFileName() {
    Instant time = Instant.now().minusMillis(mRandom.nextInt());
    return String.format(BackupManager.BACKUP_FILE_FORMAT,
        DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of("UTC")).format(time),
        time.toEpochMilli());
  }
}
