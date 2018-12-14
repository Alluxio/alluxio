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

package alluxio.server.meta;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.meta.MetaDailyBackup;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.FormatUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.regex.Matcher;

/**
 * Tests the daily primary master metadata backup.
 */
public class MetaDailyBackupIntegrationTest extends BaseIntegrationTest {
  private File mBackupDir;
  private LocalAlluxioCluster mCluster;

  @Before
  public void before() throws Exception {
    LocalDateTime backUpTime = LocalDateTime.now(Clock.systemUTC()).plusMinutes(2);
    String backUpTimeString = backUpTime.getHour() + ":" + backUpTime.getMinute();
    mBackupDir = new File("/tmp/alluxio_backups");

    // Set the daily backup properties
    mCluster = new LocalAlluxioCluster(1);
    mCluster.initConfiguration();
    Configuration.set(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, true);
    Configuration.set(PropertyKey.MASTER_DAILY_BACKUP_TIME, backUpTimeString);
    Configuration.set(PropertyKey.MASTER_BACKUP_DIRECTORY, mBackupDir.getAbsolutePath());
    Configuration.validate();
    mCluster.start();
  }

  @After
  public void after() throws Exception {
    if (mBackupDir.exists()) {
      mBackupDir.delete();
    }
    mCluster.stop();
  }

  @Test
  public void backupTest() throws Exception {
    // Make sure daily backup is executed
    Thread.sleep(FormatUtils.parseTimeSize("2min"));

    Assert.assertTrue(mBackupDir.exists());
    Assert.assertTrue(mBackupDir.isDirectory());

    // See if the backup file exists
    File[] filesList = mBackupDir.listFiles();
    boolean hasBackUpFile = false;
    for (File file : filesList) {
      if (file.isFile()) {
        Matcher matcher = MetaDailyBackup.BACKUP_FILE_PATTERN.matcher(file.getName());
        if (matcher.matches()) {
          hasBackUpFile = true;
        }
      }
    }
    Assert.assertTrue(hasBackUpFile);
  }
}
