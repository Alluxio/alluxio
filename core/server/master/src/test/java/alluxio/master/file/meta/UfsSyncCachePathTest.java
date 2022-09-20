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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DescendantType;

import com.google.common.cache.Cache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;

public class UfsSyncCachePathTest {

  private String mGrandParentDir;
  private String mParentPath;
  private String mChildPath;
  private String mChildFile;
  private UfsSyncPathCache mUspCache;

  @Before
  public void before() throws Exception {
    mGrandParentDir = "/dir1";
    mParentPath = "/dir1/dir2";
    mChildPath = "/dir1/dir2/dir3";
    mChildFile = "/dir1/dir2/file";
    int maxPaths = Configuration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY);
    mUspCache = new UfsSyncPathCache(maxPaths, Clock.systemUTC());
  }

  @Test
  public void ignoreIntervalTime() {
    // request from getFileInfo
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mParentPath, -1, true);
    Assert.assertFalse(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 0, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    // request from listStatus
    shouldSync = mUspCache.shouldSyncPath(mParentPath, -1, false);
    Assert.assertFalse(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 0, false);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The path itself is in UfsSyncCachePath.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoInCache() throws Exception {
    getFileInfoInCache(DescendantType.NONE);
    getFileInfoInCache(DescendantType.ONE);
    getFileInfoInCache(DescendantType.ALL);
  }

  private void getFileInfoInCache(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, null);
    Thread.sleep(50);
    // request from getFileInfo
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mParentPath, 30, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 10000, true);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoFromDirectParent() throws Exception {
    getFileInfoFromDirectParent(DescendantType.NONE);
    getFileInfoFromDirectParent(DescendantType.ONE);
    getFileInfoFromDirectParent(DescendantType.ALL);
  }

  private void getFileInfoFromDirectParent(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, true);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, true);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoFromGrandParentONE() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ONE, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, true);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoFromGrandParentALL() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ALL, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, true);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, true);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, true);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The path itself is in UfsSyncCachePath.
   *
   * @throws Exception
   */
  @Test
  public void listStatusInCache() throws Exception {
    listStatusInCache(DescendantType.NONE);
    listStatusInCache(DescendantType.ONE);
    listStatusInCache(DescendantType.ALL);
  }

  private void listStatusInCache(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, null);
    Thread.sleep(50);
    // request from listStatus
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mParentPath, 30, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 10000, false);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   * @throws Exception
   */
  @Test
  public void lsFromDirectParentONE() throws Exception {
    mUspCache.notifySyncedPath(mParentPath, DescendantType.ONE, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, false);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   *
   * @throws Exception
   */
  @Test
  public void lsFromDirectParentALL() throws Exception {
    mUspCache.notifySyncedPath(mParentPath, DescendantType.ALL, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, false);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, false);
    Assert.assertFalse(shouldSync.isShouldSync());
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   * @throws Exception
   */
  @Test
  public void lsFromGrandParentONE() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ONE, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, false);
    Assert.assertTrue(shouldSync.isShouldSync());
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   *
   * @throws Exception
   */
  @Test
  public void lsFromGrandParentALL() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ALL, null);
    Thread.sleep(50);
    // test child directory
    SyncCheck shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, false);
    Assert.assertFalse(shouldSync.isShouldSync());
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, false);
    Assert.assertTrue(shouldSync.isShouldSync());
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, false);
    Assert.assertFalse(shouldSync.isShouldSync());
  }
}
