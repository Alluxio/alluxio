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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests the {@link UfsSyncPathCache} using only validations and invalidations,
 * and without using interval based syncing.
 */
public class InvalidationSyncCacheTest {

  private UfsSyncPathCache mCache;
  private AtomicLong mTime;
  private Clock mClock;

  private final AlluxioURI mRoot = new AlluxioURI("/");
  private final AlluxioURI mOne = new AlluxioURI("/one");
  private final AlluxioURI mOneOne = new AlluxioURI("/one/one");
  private final AlluxioURI mTwo = new AlluxioURI("/two");
  private final AlluxioURI mTwoTwo = new AlluxioURI("/two/two");

  @Before
  public void before() {
    mClock = Mockito.mock(Clock.class);
    mTime = new AtomicLong();
    Mockito.doAnswer(invocation -> mTime.incrementAndGet()).when(mClock).millis();
    mCache = new UfsSyncPathCache(mClock, Optional::of);
  }

  @Test
  public void eviction() throws Exception {
    // max files is the number of files we will have in our cache
    // these will be under the directory /one
    int maxFiles = 100;
    Set<String> evicted = new ConcurrentSkipListSet<>();
    Set<AlluxioURI> added = new ConcurrentSkipListSet<>();
    // make a cache maxFiles + 1, to include the parent folder /one
    Configuration.set(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY, maxFiles + 1);
    mCache = new UfsSyncPathCache(mClock, Optional::of, (path, state) ->
        evicted.add(path));

    // our root sync folder /one should always stay in the cache since it is LRU, and we will
    // read it each time we check if an entry needs to be synced
    mCache.notifyInvalidation(mOne);

    // fill the cache
    for (int i = 0; i < maxFiles * 2; i++) {
      AlluxioURI nextPath = mOne.join(String.format("%03d", i));
      mCache.notifySyncedPath(nextPath, DescendantType.ALL,
          mCache.recordStartSync(), null, false);
      added.add(nextPath);
      Assert.assertFalse(mCache.shouldSyncPath(nextPath,
          Long.MAX_VALUE, DescendantType.ALL).isShouldSync());
    }
    mCache.mItems.cleanUp();
    Assert.assertEquals(maxFiles, evicted.size());
    for (String next : evicted) {
      Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI(next),
          Long.MAX_VALUE, DescendantType.ALL).isShouldSync());
    }
    for (AlluxioURI next : added) {
      if (!evicted.contains(next.getPath())) {
        Assert.assertFalse(mCache.shouldSyncPath(next,
            Long.MAX_VALUE, DescendantType.ALL).isShouldSync());
      }
    }
  }

  @Test
  public void directValidation() throws InvalidPathException {
    // no sync has happened
    Assert.assertTrue(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // after syncing with descendant type none, a sync is not needed only for
    // sync type none
    mCache.notifySyncedPath(mRoot, DescendantType.NONE,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(mRoot);
    Assert.assertTrue(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync with descendant type one
    // only sync check with type one or none should be valid
    mCache.notifySyncedPath(mRoot, DescendantType.ONE,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(mRoot);
    Assert.assertTrue(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync with descendant type all,
    // all sync checks should be valid
    mCache.notifySyncedPath(mRoot, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(mRoot);
    Assert.assertTrue(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void oneLevelValidation() throws InvalidPathException {
    // no paths are synced
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync /one with descendant type none
    mCache.notifySyncedPath(mOne, DescendantType.NONE,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(mOne);
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync / with descendant type none
    // children should not be synced
    mCache.notifySyncedPath(mRoot, DescendantType.NONE,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync / with type descendant one
    // only children with sync type none do not need to sync
    mCache.notifySyncedPath(mRoot, DescendantType.ONE,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync / with descendant type all,
    // all children sync types should be synced
    mCache.notifySyncedPath(mRoot, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate /one, ensure /one needs sync, and / needs sync for
    // descendant types not none
    mCache.notifyInvalidation(mOne);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    mCache.notifySyncedPath(mOne, DescendantType.ALL,
        mCache.recordStartSync(), null, false);

    // other files should not need sync
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate /two, ensure it needs a sync
    mCache.notifyInvalidation(mTwo);
    Assert.assertTrue(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync /two, ensure / still needs sync for descendant types not equal
    // to none
    mCache.notifySyncedPath(mTwo, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate /, all should need sync
    mCache.notifyInvalidation(mRoot);
    Assert.assertTrue(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void multiLevelValidation() throws InvalidPathException {
    // initially all need sync
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync the nested path /one/one, the parent should still need sync
    mCache.notifySyncedPath(mOneOne, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync the root, all should be synced
    mCache.notifySyncedPath(mRoot, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the nested path
    // /one should not need a sync for descendant type none
    // / should not need a sync for descendant type one or none
    mCache.notifyInvalidation(mOneOne);
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // a different path /two/two should not need sync
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwo,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwoTwo,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwoTwo,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mTwoTwo,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // syncing / with descendant type one should also mean that /one is synced
    // but only for descendant type none, and /one/one still needs sync
    mCache.notifySyncedPath(mRoot, DescendantType.ONE,
        mCache.recordStartSync(), null, false);
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // syncing / with descendant type all should also mean that /one/one is synced
    // for all descendant types
    mCache.notifySyncedPath(mRoot, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOneOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void multiLevelInvalidation() throws InvalidPathException {
    // check that invalidating a parent path also invalidates nested children
    AlluxioURI checkPath = mRoot;
    for (int i = 0; i < 10; i++) {
      mCache.notifySyncedPath(checkPath, DescendantType.ALL,
          mCache.recordStartSync(), null, false);
      Assert.assertFalse(mCache.shouldSyncPath(checkPath,
              Long.MAX_VALUE, DescendantType.NONE)
          .isShouldSync());
      Assert.assertFalse(mCache.shouldSyncPath(checkPath,
              Long.MAX_VALUE, DescendantType.ONE)
          .isShouldSync());
      Assert.assertFalse(mCache.shouldSyncPath(checkPath,
              Long.MAX_VALUE, DescendantType.ALL)
          .isShouldSync());
      // invalidate the root
      mCache.notifyInvalidation(mRoot);
      Assert.assertTrue(mCache.shouldSyncPath(checkPath,
              Long.MAX_VALUE, DescendantType.NONE)
          .isShouldSync());
      Assert.assertTrue(mCache.shouldSyncPath(checkPath,
              Long.MAX_VALUE, DescendantType.ONE)
          .isShouldSync());
      Assert.assertTrue(mCache.shouldSyncPath(checkPath,
              Long.MAX_VALUE, DescendantType.ALL)
          .isShouldSync());
      checkPath = checkPath.join("/one");
    }
  }

  @Test
  public void overactiveInvalidation() throws InvalidPathException {
    // even though a single path was invalidated, and then validated, the root still thinks
    // it needs to be validated
    // this test shows we can further improve the cache algorithm
    mCache.notifySyncedPath(mRoot, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(mOne);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(mOne, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    Assert.assertFalse(mCache.shouldSyncPath(mRoot,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mRoot, Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void invalidationAndInterval() throws Exception {
    // check that invalidations work alongside sync intervals
    // Do a sync around time 100
    mTime.set(100L);
    mCache.notifySyncedPath(mOne, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    // Check sync not needed with interval 50
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());
    // increase the time by 50, we should need a sync
    mTime.addAndGet(50);
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());
    // sync again at the new time
    mCache.notifySyncedPath(mOne, DescendantType.ALL,
        mCache.recordStartSync(), null, false);
    // sync should not be needed with interval 50
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());
    // invalidate the path, a sync should be needed
    mCache.notifyInvalidation(mOne);
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());
    // even if the time goes backwards a sync should still be needed
    mTime.set(0);
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());
  }

  @Test
  public void concurrentInvalidationTest() throws Exception {
    // an invalidation happens during a sync should mean the item still needs
    // to be synced
    long startTime = mCache.recordStartSync();
    mTime.incrementAndGet();
    mCache.notifyInvalidation(mOne);
    mCache.notifySyncedPath(mOne, DescendantType.ALL, startTime, null, false);
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());

    // same should be true with a concurrent invalidation of a child
    startTime = mCache.recordStartSync();
    mTime.incrementAndGet();
    // invalidate the child
    mCache.notifyInvalidation(mOneOne);
    mCache.notifySyncedPath(mOne, DescendantType.ALL, startTime, null, false);
    Assert.assertTrue(mCache.shouldSyncPath(mOneOne,
            50, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            50, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(mOne,
            50, DescendantType.ONE)
        .isShouldSync());
    // the parent doesn't need a sync with descendant NONE
    // since only its child was invalidated
    Assert.assertFalse(mCache.shouldSyncPath(mOne,
            50, DescendantType.NONE)
        .isShouldSync());
  }
}
