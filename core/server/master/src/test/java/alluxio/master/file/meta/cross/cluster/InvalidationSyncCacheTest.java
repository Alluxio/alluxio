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

package alluxio.master.file.meta.cross.cluster;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

public class InvalidationSyncCacheTest {

  private InvalidationSyncCache mCache;

  @Before
  public void before() {
    mCache = new InvalidationSyncCache(Optional::of);
  }

  @Test
  public void directValidation() throws InvalidPathException {
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void oneLevelValidation() throws InvalidPathException {
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.NONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ONE,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/two"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/two"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/two")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void multiLevelValidation() throws InvalidPathException {
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/one/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/one/one"));
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());

    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/two/two"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one/one"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void overactiveInvalidation() throws InvalidPathException {
    // even though a single path was invalidated, and then validated, the root still thinks
    // it needs to be validated
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifyInvalidation(new AlluxioURI("/one"));
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());

    mCache.notifySyncedPath(new AlluxioURI("/one"), DescendantType.ALL,
        mCache.startSync(new AlluxioURI("/one")), null);
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertFalse(mCache.shouldSyncPath(new AlluxioURI("/one"), 0, DescendantType.ALL)
        .isShouldSync());
  }

  @Test
  public void concurrentInvalidation() throws InvalidPathException {
    long time = mCache.startSync(new AlluxioURI("/"));
    mCache.notifyInvalidation(new AlluxioURI("/"));
    mCache.notifySyncedPath(new AlluxioURI("/"), DescendantType.ALL, time, null);
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.NONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ONE)
        .isShouldSync());
    Assert.assertTrue(mCache.shouldSyncPath(new AlluxioURI("/"), 0, DescendantType.ALL)
        .isShouldSync());
  }
}
