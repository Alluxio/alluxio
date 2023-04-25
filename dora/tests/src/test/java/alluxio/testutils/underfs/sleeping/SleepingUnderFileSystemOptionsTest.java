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

package alluxio.testutils.underfs.sleeping;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests the {@link SleepingUnderFileSystemOptions} class.
 */
public class SleepingUnderFileSystemOptionsTest {

  /**
   * Tests that the default {@link SleepingUnderFileSystemOptions} are as expected.
   */
  @Test
  public void defaults() {
    SleepingUnderFileSystemOptions defaults = new SleepingUnderFileSystemOptions();
    Assert.assertEquals(-1, defaults.getCleanupMs());
    Assert.assertEquals(-1, defaults.getCloseMs());
    Assert.assertEquals(-1, defaults.getConnectFromMasterMs());
    Assert.assertEquals(-1, defaults.getConnectFromWorkerMs());
    Assert.assertEquals(-1, defaults.getCreateMs());
    Assert.assertEquals(-1, defaults.getDeleteDirectoryMs());
    Assert.assertEquals(-1, defaults.getDeleteFileMs());
    Assert.assertEquals(-1, defaults.getGetBlockSizeByteMs());
    Assert.assertEquals(-1, defaults.getGetConfMs());
    Assert.assertEquals(-1, defaults.getGetDirectoryStatusMs());
    Assert.assertEquals(-1, defaults.getGetFileLocationsMs());
    Assert.assertEquals(-1, defaults.getGetFileStatusMs());
    Assert.assertEquals(-1, defaults.getGetSpaceMs());
    Assert.assertEquals(-1, defaults.getGetUnderFSTypeMs());
    Assert.assertEquals(-1, defaults.getIsDirectoryMs());
    Assert.assertEquals(-1, defaults.getIsFileMs());
    Assert.assertEquals(-1, defaults.getListStatusMs());
    Assert.assertEquals(-1, defaults.getMkdirsMs());
    Assert.assertEquals(-1, defaults.getOpenMs());
    Assert.assertEquals(-1, defaults.getRenameDirectoryMs());
    Assert.assertEquals(-1, defaults.getRenameFileMs());
    Assert.assertEquals(-1, defaults.getRenameTemporaryFileMs());
    Assert.assertEquals(-1, defaults.getSetConfMs());
    Assert.assertEquals(-1, defaults.getSetModeMs());
    Assert.assertEquals(-1, defaults.getSetOwnerMs());
    Assert.assertEquals(-1, defaults.getSupportsFlushMs());
  }

  /**
   * Tests that the setters of {@link SleepingUnderFileSystemOptions} work as expected.
   */
  @Test
  public void fields() {
    SleepingUnderFileSystemOptions defaults = new SleepingUnderFileSystemOptions();
    Random random = new Random();
    long sleepCleanupMs = random.nextLong();
    long sleepCloseMs = random.nextLong();
    long sleepConnectFromMasterMs = random.nextLong();
    long sleepConnectFromWorkerMs = random.nextLong();
    long sleepCreateMs = random.nextLong();
    long sleepDeleteDirectoryMs = random.nextLong();
    long sleepDeleteFileMs = random.nextLong();
    long sleepGetBlockSizeByteMs = random.nextLong();
    long sleepGetConfMs = random.nextLong();
    long sleepGetDirectoryStatusMs = random.nextLong();
    long sleepGetFileLocationsMs = random.nextLong();
    long sleepGetFileStatusMs = random.nextLong();
    long sleepGetSpaceMs = random.nextLong();
    long sleepGetUnderFSTypeMs = random.nextLong();
    long sleepIsDirectoryMs = random.nextLong();
    long sleepIsFileMs = random.nextLong();
    long sleepListStatusMs = random.nextLong();
    long sleepMkdirsMs = random.nextLong();
    long sleepOpenMs = random.nextLong();
    long sleepRenameDirectoryMs = random.nextLong();
    long sleepRenameFileMs = random.nextLong();
    long sleepRenameTemporaryFileMs = random.nextLong();
    long sleepSetConfMs = random.nextLong();
    long sleepSetModeMs = random.nextLong();
    long sleepSetOwnerMs = random.nextLong();
    long sleepSupportsFlushMs = random.nextLong();

    defaults
        .setCleanupMs(sleepCleanupMs)
        .setCloseMs(sleepCloseMs)
        .setConnectFromMasterMs(sleepConnectFromMasterMs)
        .setConnectFromWorkerMs(sleepConnectFromWorkerMs)
        .setCreateMs(sleepCreateMs)
        .setDeleteDirectoryMs(sleepDeleteDirectoryMs)
        .setDeleteFileMs(sleepDeleteFileMs)
        .setGetBlockSizeByteMs(sleepGetBlockSizeByteMs)
        .setGetConfMs(sleepGetConfMs)
        .setGetDirectoryStatusMs(sleepGetDirectoryStatusMs)
        .setGetFileLocationsMs(sleepGetFileLocationsMs)
        .setGetFileStatusMs(sleepGetFileStatusMs)
        .setGetSpaceMs(sleepGetSpaceMs)
        .setGetUnderFSTypeMs(sleepGetUnderFSTypeMs)
        .setIsDirectoryMs(sleepIsDirectoryMs)
        .setIsFileMs(sleepIsFileMs)
        .setListStatusMs(sleepListStatusMs)
        .setMkdirsMs(sleepMkdirsMs)
        .setOpenMs(sleepOpenMs)
        .setRenameDirectoryMs(sleepRenameDirectoryMs)
        .setRenameFileMs(sleepRenameFileMs)
        .setRenameTemporaryFileMs(sleepRenameTemporaryFileMs)
        .setSetConfMs(sleepSetConfMs)
        .setSetModeMs(sleepSetModeMs)
        .setSetOwnerMs(sleepSetOwnerMs)
        .setSupportsFlushMs(sleepSupportsFlushMs);

    Assert.assertEquals(sleepCleanupMs, defaults.getCleanupMs());
    Assert.assertEquals(sleepCloseMs, defaults.getCloseMs());
    Assert.assertEquals(sleepConnectFromMasterMs, defaults.getConnectFromMasterMs());
    Assert.assertEquals(sleepConnectFromWorkerMs, defaults.getConnectFromWorkerMs());
    Assert.assertEquals(sleepCreateMs, defaults.getCreateMs());
    Assert.assertEquals(sleepDeleteDirectoryMs, defaults.getDeleteDirectoryMs());
    Assert.assertEquals(sleepDeleteFileMs, defaults.getDeleteFileMs());
    Assert.assertEquals(sleepGetBlockSizeByteMs, defaults.getGetBlockSizeByteMs());
    Assert.assertEquals(sleepGetConfMs, defaults.getGetConfMs());
    Assert.assertEquals(sleepGetDirectoryStatusMs, defaults.getGetDirectoryStatusMs());
    Assert.assertEquals(sleepGetFileLocationsMs, defaults.getGetFileLocationsMs());
    Assert.assertEquals(sleepGetFileStatusMs, defaults.getGetFileStatusMs());
    Assert.assertEquals(sleepGetSpaceMs, defaults.getGetSpaceMs());
    Assert.assertEquals(sleepGetUnderFSTypeMs, defaults.getGetUnderFSTypeMs());
    Assert.assertEquals(sleepIsDirectoryMs, defaults.getIsDirectoryMs());
    Assert.assertEquals(sleepIsFileMs, defaults.getIsFileMs());
    Assert.assertEquals(sleepListStatusMs, defaults.getListStatusMs());
    Assert.assertEquals(sleepMkdirsMs, defaults.getMkdirsMs());
    Assert.assertEquals(sleepOpenMs, defaults.getOpenMs());
    Assert.assertEquals(sleepRenameDirectoryMs, defaults.getRenameDirectoryMs());
    Assert.assertEquals(sleepRenameFileMs, defaults.getRenameFileMs());
    Assert.assertEquals(sleepRenameTemporaryFileMs, defaults.getRenameTemporaryFileMs());
    Assert.assertEquals(sleepSetConfMs, defaults.getSetConfMs());
    Assert.assertEquals(sleepSetModeMs, defaults.getSetModeMs());
    Assert.assertEquals(sleepSetOwnerMs, defaults.getSetOwnerMs());
    Assert.assertEquals(sleepSupportsFlushMs, defaults.getSupportsFlushMs());
  }
}
