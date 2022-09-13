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

/**
 * Tests the {@link InvalidationSyncCache} using only validations and invalidations,
 * without using interval based syncing.
 */
public class InvalidationSyncCacheTest {

  private InvalidationSyncCache mCache;
  private Long[] mClockTime;

  @Before
  public void before() {
    mClockTime = new Long[] {0L};
    Clock clock = Mockito.mock(Clock.class);
    Mockito.doAnswer(ignored -> mClockTime[0]).when(clock).millis();
    mCache = new InvalidationSyncCache(clock, Optional::of);
  }

  @Test
  public void directValidation() throws InvalidPathException {
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 1L;
    // sync at time 1
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 2L;
    // invalidation at time 2
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 3L;
    // sync at time 3
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] =  4L;
    // invalidation at time 4
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 5L;
    // sync at time 5
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 6L;
    // invalidation at time 6
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void oneLevelValidation() throws InvalidPathException {
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 1L;
    // sync at time 1
    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 2L;
    // invalidation at time 2
    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 3L;
    // sync at time 3
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 4L;
    // sync at time 4 with descendant one
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 5L;
    // sync at time 5 with descendant all
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 6L;
    // invalidation at time 6
    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 7L;
    // invalidation at time 7
    mCache.notifyInvalidation(new AlluxioURI("/two"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 8L;
    // invalidation at time 8
    mCache.notifySyncedPath(new AlluxioURI("/two"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/two")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 9L;
    // invalidation at time 9
    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void multiLevelValidation() throws InvalidPathException {
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 1L;
    // sync at time 1
    mCache.notifySyncedPath(new AlluxioURI("/one/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 2L;
    // sync at time 2
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 3L;
    // invalidation at time 3
    mCache.notifyInvalidation(new AlluxioURI("/one/one"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());

    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 4L;
    // sync at time 4
    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void overactiveInvalidation() throws InvalidPathException {
    // even though a single path was invalidated, and then validated, the root still thinks
    // it needs to be validated
    // this test shows we can further improve the cache algorithm
    mClockTime[0] = 1L;
    // sync at time 1
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 2L;
    // invalidate at time 2
    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());

    mClockTime[0] = 3L;
    // sync at time 3
    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 100, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void concurrentInvalidation() throws InvalidPathException {
    // TODO(tcrain) this test will be used with cross cluster sync PR
    long time = mCache.startSync(new AlluxioURI("/"));
    mCache.notifyInvalidation(new AlluxioURI("/"));
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL, time, null);
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 100, DescendantType.ALL)
        .isShouldSync());
  }
}
