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

package alluxio.client.file.cache;

import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.util.io.BufferUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

public class TimeBoundPageStoreTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int CACHE_SIZE_BYTES = 512 * Constants.KB;
  private static final PageId PAGE_ID = new PageId("0L", 0L);
  private static final byte[] PAGE = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private byte[] mBuf = new byte[PAGE_SIZE_BYTES];
  private PageStoreOptions mPageStoreOptions;
  private HangingPageStore mPageStore;
  private TimeBoundPageStore mTimeBoundPageStore;
  private PageStoreOptions mTimeBoundPageStoreOptions;

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    conf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
    conf.set(PropertyKey.USER_CLIENT_CACHE_DIR, mTemp.getRoot().getAbsolutePath());
    conf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "-1");
    mPageStoreOptions = PageStoreOptions.create(conf);
    mPageStore = new HangingPageStore(mPageStoreOptions);

    conf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "2s");
    mTimeBoundPageStoreOptions = PageStoreOptions.create(conf);
    mTimeBoundPageStore = new TimeBoundPageStore(mPageStore, mTimeBoundPageStoreOptions);
  }

  @Test
  public void put() throws Exception {
    mTimeBoundPageStore.put(PAGE_ID, PAGE);
    assertEquals(PAGE.length, mPageStore.get(PAGE_ID, 0, PAGE.length, mBuf, 0));
    assertArrayEquals(PAGE, mBuf);
  }

  @Test
  public void get() throws Exception {
    mPageStore.put(PAGE_ID, PAGE);
    assertEquals(PAGE.length, mTimeBoundPageStore.get(PAGE_ID, 0, PAGE.length, mBuf, 0));
    assertArrayEquals(PAGE, mBuf);
  }

  @Test
  public void delete() throws Exception {
    mPageStore.put(PAGE_ID, PAGE);
    mTimeBoundPageStore.delete(PAGE_ID);
    assertThrows(PageNotFoundException.class, () ->
        mPageStore.get(PAGE_ID, 0, PAGE.length, mBuf, 0));
  }

  @Test
  public void putTimeout() throws Exception {
    mPageStore.setPutHanging(true);
    try {
      mTimeBoundPageStore.put(PAGE_ID, PAGE);
      fail();
    } catch (IOException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void getTimeout() throws Exception {
    mPageStore.setGetHanging(true);
    try {
      mTimeBoundPageStore.get(PAGE_ID, 0, PAGE.length, mBuf, 0);
      fail();
    } catch (IOException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void deleteTimeout() throws Exception {
    mPageStore.setDeleteHanging(true);
    try {
      mTimeBoundPageStore.delete(PAGE_ID);
      fail();
    } catch (IOException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void concurrentPutWithLargeThreadPool() throws Exception {
    final int threadPoolSize = 10;
    mTimeBoundPageStoreOptions.setTimeoutThreads(threadPoolSize);
    mTimeBoundPageStore = new TimeBoundPageStore(mPageStore, mTimeBoundPageStoreOptions);
    Exception[] exceptions = concurrentAccess(threadPoolSize);
    for (int i = 0; i < threadPoolSize; i++) {
      assertNull(exceptions[i]);
    }
  }

  @Test
  public void concurrentPutWithSmallThreadPool() throws Exception {
    final int threadPoolSize = 10;
    final int concurrency = 20;
    mTimeBoundPageStoreOptions.setTimeoutThreads(threadPoolSize);
    mTimeBoundPageStore = new TimeBoundPageStore(mPageStore, mTimeBoundPageStoreOptions);
    mPageStore.setPutHanging(true);
    Exception[] exceptions = concurrentAccess(concurrency);
    int timeoutExceptions = 0;
    int rejectetdExecutionExceptions = 0;
    for (int i = 0; i < concurrency; i++) {
      if (!(exceptions[i] instanceof IOException)) {
        fail(exceptions[i].toString());
      }
      if (exceptions[i].getCause() instanceof TimeoutException) {
        timeoutExceptions++;
      } else if (exceptions[i].getCause() instanceof RejectedExecutionException) {
        rejectetdExecutionExceptions++;
      } else {
        fail();
      }
    }
    assertEquals(concurrency - threadPoolSize, rejectetdExecutionExceptions);
    assertEquals(threadPoolSize, timeoutExceptions);
  }

  private Exception[] concurrentAccess(int threads) throws Exception {
    ExecutorService executor = newScheduledThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>();
    Exception[] exceptions = new Exception[threads];
    for (int i = 0; i < threads; i++) {
      PageId pageId = new PageId("0L", i);
      int index = i;
      futures.add(executor.submit(() -> {
        try {
          mTimeBoundPageStore.put(pageId, PAGE);
        } catch (Exception e) {
          exceptions[index] = e;
        }
      }));
    }

    for (Future<?> future : futures) {
      future.get();
    }
    return exceptions;
  }
}
