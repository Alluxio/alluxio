/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.replay;

import alluxio.exception.AlluxioException;
import alluxio.exception.AlluxioExceptionType;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.replay.ReplayCache.ReplayCallable;
import alluxio.replay.ReplayCache.ReplayCallableThrowsIOException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.ThriftIOException;

import com.google.common.base.Throwables;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link ReplayCache}.
 */
public final class ReplayCacheTest {
  private static final Long TEST_MAX_SIZE = Long.MAX_VALUE;
  private static final Long TEST_TIMEOUT_MS = Long.MAX_VALUE;
  private static final String TEST_ERROR_MESSAGE = "test error message in ReplayCacheTest";

  private ReplayCache<Long> mCache;

  /**
   * Sets up a new {@link ReplayCache} instance before a test runs.
   */
  @Before
  public void before() {
    mCache = ReplayCache.newInstance(TEST_MAX_SIZE, TEST_TIMEOUT_MS);
  }

  /**
   * Tests that the {@link ReplayCallable} is only called when the key doesn't exist in the cache.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testCaching1() throws Exception {
    CounterCallable counter = new CounterCallable();
    Assert.assertEquals(1L, (long) mCache.run("key1", counter));
    // Should return the first value for key1
    Assert.assertEquals(1L, (long) mCache.run("key1", counter));
    // Should re-run on a different key
    Assert.assertEquals(2L, (long) mCache.run("key2", counter));
    // Should re-run on a different key
    Assert.assertEquals(3L, (long) mCache.run("key3", counter));
    // Should return the old value for an old key
    Assert.assertEquals(2L, (long) mCache.run("key2", counter));
  }

  /**
   * Tests that the {@link ReplayCallableThrowsIOException} is only called when the key doesn't
   * exist in the cache.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testCaching2() throws Exception {
    CounterCallableThrowsIOException counter = new CounterCallableThrowsIOException();
    Assert.assertEquals(1L, (long) mCache.run("key1", counter));
    // Should return the first value for key1
    Assert.assertEquals(1L, (long) mCache.run("key1", counter));
    // Should re-run on a different key
    Assert.assertEquals(2L, (long) mCache.run("key2", counter));
    // Should re-run on a different key
    Assert.assertEquals(3L, (long) mCache.run("key3", counter));
    // Should return the old value for an old key
    Assert.assertEquals(2L, (long) mCache.run("key2", counter));
  }

  /**
   * Tests that old keys are evicted when the configured maximum size is exceeded.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testSizeEviction() throws Exception {
    // Create a cache with max size 2.
    ReplayCache<Long> cache = ReplayCache.newInstance(2, TEST_TIMEOUT_MS);
    CounterCallable counter = new CounterCallable();
    Assert.assertEquals(1, (long) cache.run("key1", counter));
    Assert.assertEquals(2, (long) cache.run("key2", counter));
    Assert.assertEquals(3, (long) cache.run("key3", counter));
    // key1 is evicted when key3 is added
    Assert.assertEquals(4, (long) cache.run("key1", counter));
  }

  /**
   * Tests that keys are evicted after the configured timeout.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testTimeEviction() throws Exception {
    // Create a cache with timeout of 10ms
    ReplayCache<Long> cache = ReplayCache.newInstance(TEST_MAX_SIZE, 10);
    CounterCallable counter = new CounterCallable();
    Assert.assertEquals(1, (long) cache.run("key1", counter));
    Thread.sleep(11);
    // key1 is evicted after 10ms
    Assert.assertEquals(2, (long) cache.run("key1", counter));
  }

  /**
   * Tests for {@link ReplayCallable}s that {@link AlluxioException}s are properly re-thrown as
   * {@link AlluxioTException}s.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testAlluxioExceptionRethrow1() throws Exception {
    try {
      mCache.run("key", new ThrowingCallable(new FileAlreadyExistsException(TEST_ERROR_MESSAGE)));
      Assert.fail("Should have thrown AlluxioTException");
    } catch (AlluxioTException e) {
      Assert.assertEquals(TEST_ERROR_MESSAGE, e.getMessage());
      Assert.assertEquals(AlluxioExceptionType.FILE_ALREADY_EXISTS.name(), e.getType());
    }
  }

  /**
   * Tests for {@link ReplayCallableThrowsIOException}s that {@link AlluxioException}s are properly
   * rethrown as {@link AlluxioTException}s.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testAlluxioExceptionRethrow2() throws Exception {
    try {
      mCache.run("key", new ThrowingCallableThrowsIOException(
          new FileAlreadyExistsException(TEST_ERROR_MESSAGE)));
      Assert.fail("Should have thrown AlluxioTException");
    } catch (AlluxioTException e) {
      Assert.assertEquals(TEST_ERROR_MESSAGE, e.getMessage());
      Assert.assertEquals(AlluxioExceptionType.FILE_ALREADY_EXISTS.name(), e.getType());
    }
  }

  /**
   * Tests for {@link ReplayCallableThrowsIOException}s that {@link IOException}s are correctly
   * re-thrown as {@link ThriftIOException}s.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testIOExceptionRethrow() throws Exception {
    try {
      mCache.run("key", new ThrowingCallableThrowsIOException(new IOException(TEST_ERROR_MESSAGE)));
      Assert.fail("Should have thrown ThriftIOException");
    } catch (ThriftIOException e) {
      Assert.assertEquals(TEST_ERROR_MESSAGE, e.getMessage());
    }
  }

  /**
   * Tests for {@link ReplayCallable}s that {@link RuntimeException}s are correctly propagated.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testRuntimeExceptionPropagated1() throws Exception {
    RuntimeException exception = new RuntimeException(TEST_ERROR_MESSAGE);
    try {
      mCache.run("key", new ThrowingCallable(exception));
      Assert.fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      Assert.assertSame(exception, e);
    }
  }

  /**
   * Tests for {@link ReplayCallableThrowsIOException}s that {@link RuntimeException}s are correctly
   * propagated.
   *
   * @throws Exception when the replay logic fails
   */
  @Test
  public void testRuntimeExceptionPropagated2() throws Exception {
    RuntimeException exception = new RuntimeException(TEST_ERROR_MESSAGE);
    try {
      mCache.run("key", new ThrowingCallableThrowsIOException(exception));
      Assert.fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      Assert.assertSame(exception, e);
    }
  }

  /**
   * Returns 1, then 2, then 3, etc.
   */
  private class CounterCallable implements ReplayCallable<Long> {
    private long mCount = 0;

    @Override
    public Long call() throws AlluxioException {
      return ++ mCount;
    }
  }

  /**
   * Same as CounterCallable, but for {@link ReplayCallableThrowsIOException}.
   */
  private class CounterCallableThrowsIOException implements ReplayCallableThrowsIOException<Long> {
    private long mCount = 0;

    @Override
    public Long call() throws AlluxioException, IOException {
      return ++ mCount;
    }
  }

  /**
   * Class which throws the given {@link Exception}, which should be either a
   * {@link AlluxioException} or {@link RuntimeException}.
   */
  private class ThrowingCallable implements ReplayCallable<Long> {
    private final Exception mException;

    public ThrowingCallable(Exception t) {
      mException = t;
    }

    @Override
    public Long call() throws AlluxioException {
      // If it's a AlluxioException, don't wrap it in RuntimeException
      Throwables.propagateIfInstanceOf(mException, AlluxioException.class);
      throw Throwables.propagate(mException);
    }
  }

  /**
   * Class which throws the given {@link Exception}, which should be a {@link AlluxioException},
   * {@link IOException}, or {@link RuntimeException}.
   */
  private class ThrowingCallableThrowsIOException implements ReplayCallableThrowsIOException<Long> {
    private final Exception mException;

    public ThrowingCallableThrowsIOException(Exception t) {
      mException = t;
    }

    @Override
    public Long call() throws AlluxioException, IOException {
      // If it's a AlluxioException or IOException, don't wrap it in RuntimeException
      Throwables.propagateIfInstanceOf(mException, AlluxioException.class);
      Throwables.propagateIfInstanceOf(mException, IOException.class);
      throw Throwables.propagate(mException);
    }
  }
}
