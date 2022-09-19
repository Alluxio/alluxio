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
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests the {@link InvalidationSyncCache} using only validations and invalidations,
 * and without using interval based syncing.
 */
public class InvalidationSyncCacheTest {

  private InvalidationSyncCache mCache;

  @Before
  public void before() {
    Clock clock = Mockito.mock(Clock.class);
    AtomicLong time = new AtomicLong();
    Mockito.doAnswer(invocation -> time.incrementAndGet()).when(clock).millis();
    mCache = new InvalidationSyncCache(clock, Optional::of);
  }

  @Test
  public void directValidation() throws InvalidPathException {
    // no sync has happened
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // after syncing with descendant type none, a sync is not needed only for
    // sync type none
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync with descendant type one
    // only sync check with type one or none should be valid
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync with descendant type all,
    // all sync checks should be valid
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void oneLevelValidation() throws InvalidPathException {
    // no paths are synced
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync /one with descendant type none
    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the sync
    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync / with descendant type none
    // children should not be synced
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync / with type descendant one
    // only children with sync type none do not need to sync
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync / with descendant type all,
    // all children sync types should be synced
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate /one, ensure /one needs sync, and / needs sync for
    // descendant types not none
    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one")), null);

    // other files should not need sync
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate /two, ensure it needs a sync
    mCache.notifyInvalidation(new AlluxioURI("/two"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync /two, ensure / still needs sync for descendant types not equal
    // to none
    mCache.notifySyncedPath(new AlluxioURI("/two"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/two")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate /, all should need sync
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void multiLevelValidation() throws InvalidPathException {
    // initially all need sync
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync the nested path /one/one, the parent should still need sync
    mCache.notifySyncedPath(new AlluxioURI("/one/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // sync the root, all should be synced
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // invalidate the nested path
    // /one should not need a sync for descendant type none
    // / should not need a sync for descendant type one or none
    mCache.notifyInvalidation(new AlluxioURI("/one/one"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // a different path /two/two should not need sync
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // syncing / with descendant type one should also mean that /one is synced
    // but only for descendant type none, and /one/one still needs sync
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    // syncing / with descendant type all should also mean that /one/one is synced
    // for all descendant types
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void multiLevelInvalidation() throws InvalidPathException {
    // check that invalidating a parent path also invalidates nested children
    AlluxioURI checkPath = new AlluxioURI("/");
    for (int i = 0; i < 10; i++) {
      mCache.notifySyncedPath(checkPath, DescendantType.ALL,
          mCache.startSync(checkPath), null);
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
      mCache.notifyInvalidation(new AlluxioURI("/"));
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
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"),
            Long.MAX_VALUE, DescendantType.ALL)
        .isShouldSync());
  }
}
