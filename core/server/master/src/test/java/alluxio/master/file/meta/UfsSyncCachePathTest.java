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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.Optional;

public class UfsSyncCachePathTest {

  private AlluxioURI mGrandParentDir;
  private AlluxioURI mParentPath;
  private AlluxioURI mChildPath;
  private AlluxioURI mChildFile;
  private AlluxioURI mFileOne;
  private InvalidationSyncCache mUspCache;

  @Before
  public void before() throws Exception {
    mGrandParentDir = new AlluxioURI("/dir1");
    mParentPath = new AlluxioURI("/dir1/dir2");
    mChildPath = new AlluxioURI("/dir1/dir2/dir3");
    mChildFile = new AlluxioURI("/dir1/dir2/file");
    mFileOne = new AlluxioURI("/one");
    mUspCache = new InvalidationSyncCache(Clock.systemUTC(), Optional::of);
  }

  @Test
  public void ignoreIntervalTime() throws Exception {
    // request from getFileInfo
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mParentPath, -1, DescendantType.ONE);
    Assert.assertFalse(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 0, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
    // request from listStatus
    shouldSync = mUspCache.shouldSyncPath(mParentPath, -1, DescendantType.ALL);
    Assert.assertFalse(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 0, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The path itself is in UfsSyncCachePath.
   */
  @Test
  public void getFileInfoInCache() throws Exception {
    getFileInfoInCache(DescendantType.NONE);
    getFileInfoInCache(DescendantType.ONE);
    getFileInfoInCache(DescendantType.ALL);
  }

  private void getFileInfoInCache(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, mUspCache.recordStartSync(),
        null, false);
    Thread.sleep(50);
    // request from getFileInfo
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mParentPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());

    for (DescendantType syncCheckType : Arrays.asList(DescendantType.NONE, DescendantType.ONE,
        DescendantType.ALL)) {
      shouldSync = mUspCache.shouldSyncPath(mParentPath, 10000, syncCheckType);
      Assert.assertEquals(syncNeeded(descendantType, syncCheckType), shouldSync.isShouldSync());
    }
  }

  boolean syncNeeded(DescendantType lastSyncType, DescendantType syncCheckType) {
    switch (lastSyncType) {
      case NONE:
        return syncCheckType != DescendantType.NONE;
      case ONE:
        return syncCheckType != DescendantType.NONE && syncCheckType != DescendantType.ONE;
      default:
        return false;
    }
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath.
   */
  @Test
  public void getFileInfoFromDirectParent() throws Exception {
    getFileInfoFromDirectParent(DescendantType.NONE);
    getFileInfoFromDirectParent(DescendantType.ONE);
    getFileInfoFromDirectParent(DescendantType.ALL);
  }

  private void getFileInfoFromDirectParent(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, mUspCache.recordStartSync(),
        null, false);
    Thread.sleep(50);

    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.NONE);
    Assert.assertTrue(shouldSync.isShouldSync());

    for (DescendantType syncCheckType : Arrays.asList(DescendantType.NONE, DescendantType.ONE,
        DescendantType.ALL)) {
      shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, syncCheckType);
      Assert.assertEquals(syncNeededParentSync(descendantType, syncCheckType),
          shouldSync.isShouldSync());
    }
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.NONE);
    Assert.assertTrue(shouldSync.isShouldSync());

    for (DescendantType syncCheckType : Arrays.asList(DescendantType.NONE, DescendantType.ONE,
        DescendantType.ALL)) {
      shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, syncCheckType);
      Assert.assertEquals(syncNeededParentSync(descendantType, syncCheckType),
          shouldSync.isShouldSync());
    }
  }

  boolean syncNeededParentSync(DescendantType lastParentSync, DescendantType syncCheckType) {
    switch (lastParentSync) {
      case NONE:
        return true;
      case ONE:
        return syncCheckType != DescendantType.NONE;
      default:
        return false;
    }
  }

  /**
   * The grandparent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   */
  @Test
  public void getFileInfoFromGrandParentONE() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ONE,
        mUspCache.recordStartSync(), null, false);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The grandparent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   */
  @Test
  public void getFileInfoFromGrandParentALL() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ALL,
        mUspCache.recordStartSync(), null, false);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The path itself is in UfsSyncCachePath.
   */
  @Test
  public void listStatusInCache() throws Exception {
    listStatusInCache(DescendantType.NONE);
    listStatusInCache(DescendantType.ONE);
    listStatusInCache(DescendantType.ALL);
  }

  private void listStatusInCache(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, mUspCache.recordStartSync(),
        null, false);
    Thread.sleep(50);
    // request from listStatus
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mParentPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync.isShouldSync());

    for (DescendantType syncCheckType : Arrays.asList(DescendantType.NONE, DescendantType.ONE,
        DescendantType.ALL)) {
      shouldSync = mUspCache.shouldSyncPath(mParentPath, 10000, syncCheckType);
      Assert.assertEquals(syncNeeded(descendantType, syncCheckType), shouldSync.isShouldSync());
    }
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   */
  @Test
  public void lsFromDirectParentONE() throws Exception {
    mUspCache.notifySyncedPath(mParentPath, DescendantType.ONE, mUspCache.recordStartSync(),
        null, false);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   */
  @Test
  public void lsFromDirectParentALL() throws Exception {
    mUspCache.notifySyncedPath(mParentPath, DescendantType.ALL, mUspCache.recordStartSync(),
        null, false);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The grandparent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   */
  @Test
  public void lsFromGrandParentONE() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ONE,
        mUspCache.recordStartSync(), null, false);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The grandparent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   */
  @Test
  public void lsFromGrandParentALL() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ALL,
        mUspCache.recordStartSync(), null, false);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  @Test
  public void syncFileValidationTest() throws Exception {
    // if a file is synced, then any descendant type sync
    // check should succeed
    mUspCache.notifySyncedPath(mFileOne, DescendantType.NONE,
        mUspCache.recordStartSync(), null, true);
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    // but this should not be true with a directory
    // sync a directory with descendant type none
    mUspCache.notifySyncedPath(mFileOne, DescendantType.NONE,
        mUspCache.recordStartSync(), null, false);
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    // sync a directory with descendant type one
    mUspCache.notifySyncedPath(mFileOne, DescendantType.ONE,
        mUspCache.recordStartSync(), null, false);
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    // sync a directory with descendant type all
    mUspCache.notifySyncedPath(mFileOne, DescendantType.ALL,
        mUspCache.recordStartSync(), null, false);
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mUspCache.shouldSyncPath(mFileOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }
}
