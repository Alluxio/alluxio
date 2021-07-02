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

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BackupPStatus;
import alluxio.grpc.BackupState;
import alluxio.master.BackupManager;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ControllableScheduler;
import alluxio.util.io.PathUtils;
import alluxio.wire.BackupStatus;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests the {@link DailyMetadataBackup}.
 */
public class DailyMetadataBackupTest {
  private MetaMaster mMetaMaster;
  private ControllableScheduler mScheduler;
  private UnderFileSystem mUfs;
  private UfsManager mUfsManager;
  private UfsManager.UfsClient mUfsClient;
  private Random mRandom;
  private String mBackupDir;

  @Before
  public void before() throws Exception {
    mRandom = new Random();
    mBackupDir = "/tmp/test/alluxio_backups";

    mMetaMaster = Mockito.mock(MetaMaster.class);
    when(mMetaMaster.backup(any(), any()))
        .thenReturn(BackupStatus.fromProto(BackupPStatus.newBuilder()
            .setBackupId(UUID.randomUUID().toString()).setBackupState(BackupState.Completed)
            .setBackupUri(PathUtils.concatPath(mBackupDir, generateBackupFileName()))
            .setBackupHost("localhost").build()));

    mUfs = Mockito.mock(UnderFileSystem.class);
    when(mUfs.getUnderFSType()).thenReturn("local");
    when(mUfs.deleteFile(any())).thenReturn(true);

    mUfsClient = Mockito.mock(UfsManager.UfsClient.class);
    when(mUfsClient.acquireUfsResource()).thenReturn(new CloseableResource<UnderFileSystem>(mUfs) {
      @Override
      public void close() {
        // Noop
      }
    });
    mUfsManager = Mockito.mock(UfsManager.class);
    when(mUfsManager.getRoot()).thenReturn(mUfsClient);

    mScheduler = new ControllableScheduler();
  }

  @Test
  public void test() throws Exception {
    int fileToRetain = 1;
    try (Closeable c =
        new ConfigurationRule(ImmutableMap.of(
            PropertyKey.MASTER_BACKUP_DIRECTORY, mBackupDir,
            PropertyKey.MASTER_DAILY_BACKUP_ENABLED, "true",
            PropertyKey.MASTER_DAILY_BACKUP_FILES_RETAINED,
            String.valueOf(fileToRetain)), ServerConfiguration.global()).toResource()) {
      DailyMetadataBackup dailyBackup =
          new DailyMetadataBackup(mMetaMaster, mScheduler, mUfsManager);
      dailyBackup.start();

      int backUpFileNum = 0;
      when(mUfs.listStatus(mBackupDir)).thenReturn(generateUfsStatuses(++backUpFileNum));
      mScheduler.jumpAndExecute(1, TimeUnit.DAYS);
      verify(mMetaMaster, times(backUpFileNum)).backup(any(), any());
      int deleteFileNum = getNumOfDeleteFile(backUpFileNum, fileToRetain);
      verify(mUfs, times(deleteFileNum)).deleteFile(any());

      when(mUfs.listStatus(mBackupDir)).thenReturn(generateUfsStatuses(++backUpFileNum));
      mScheduler.jumpAndExecute(1, TimeUnit.DAYS);
      verify(mMetaMaster, times(backUpFileNum)).backup(any(), any());
      deleteFileNum += getNumOfDeleteFile(backUpFileNum, fileToRetain);
      verify(mUfs, times(deleteFileNum)).deleteExistingFile(any());

      when(mUfs.listStatus(mBackupDir)).thenReturn(generateUfsStatuses(++backUpFileNum));
      mScheduler.jumpAndExecute(1, TimeUnit.DAYS);
      verify(mMetaMaster, times(backUpFileNum)).backup(any(), any());
      deleteFileNum += getNumOfDeleteFile(backUpFileNum, fileToRetain);
      verify(mUfs, times(deleteFileNum)).deleteExistingFile(any());
    }
  }

  /**
   * Generates ufs statues.
   *
   * @param num the number of backup files that exist in the returned statuses
   * @return the random generated ufs file statuses
   */
  private UfsStatus[] generateUfsStatuses(int num) {
    UfsStatus[] statuses = new UfsFileStatus[num];
    for (int i = 0; i < statuses.length; i++) {
      statuses[i] = new UfsFileStatus(generateBackupFileName(),
          CommonUtils.randomAlphaNumString(10), mRandom.nextLong(), mRandom.nextLong(),
          CommonUtils.randomAlphaNumString(10), CommonUtils.randomAlphaNumString(10),
          (short) mRandom.nextInt(), mRandom.nextLong());
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

  /**
   * Gets the number of files that should be deleted.
   *
   * @param total the total number of files
   * @param retain the number of files that should be retained
   * @return the number of files that should be deleted
   */
  private int getNumOfDeleteFile(int total, int retain) {
    int diff = total - retain;
    return diff >= 0 ? diff : 0;
  }
}
