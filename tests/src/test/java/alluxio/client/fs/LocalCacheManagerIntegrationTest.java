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

package alluxio.client.fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;

// TODO(binfan): this is not a real integration test, should be consolidated with UT
public final class LocalCacheManagerIntegrationTest extends BaseIntegrationTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int PAGE_COUNT = 32;
  private static final int CACHE_SIZE_BYTES = PAGE_COUNT * PAGE_SIZE_BYTES;
  private static final PageId PAGE_ID = new PageId("0", 0L);
  private static final byte[] PAGE = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf;
  private final byte[] mBuffer = new byte[PAGE_SIZE_BYTES];

  @Before
  public void before() throws Exception {
    mConf = new InstancedConfiguration(new AlluxioProperties());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIR, mTemp.getRoot().getPath());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0);
  }

  @After
  public void after() throws Exception {
    if (mCacheManager != null) {
      mCacheManager.close();
    }
  }

  @Test
  public void newCacheRocks() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "ROCKS");
    testNewCache();
  }

  @Test
  public void newCacheLocal() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    testNewCache();
  }

  private void testNewCache() throws Exception {
    mCacheManager = LocalCacheManager.create(mConf);
    mCacheManager.put(PAGE_ID, PAGE);
    testPageCached();
  }

  private void testPageCached() throws Exception {
    testPageCached(PAGE_ID);
  }

  private void testPageCached(PageId pageId) throws Exception {
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(pageId, PAGE_SIZE_BYTES, mBuffer, 0));
    assertArrayEquals(PAGE, mBuffer);
  }

  @Test
  public void loadCacheRocks() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "ROCKS");
    testLoadCache();
  }

  @Test
  public void loadCacheLocal() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    testLoadCache();
  }

  @Test
  public void loadCacheAndEvict() throws Exception {
    loadFullCache();
    mCacheManager.close();
    // creates with same configuration
    mCacheManager = LocalCacheManager.create(mConf);
    // evicts half of the pages
    for (int i = 0; i < PAGE_COUNT / 2; i++) {
      mCacheManager.put(new PageId("1", i), PAGE);
    }
    int evicted = 0;
    for (int i = 0; i < PAGE_COUNT; i++) {
      PageId pageId = new PageId("0", i);
      int ret = mCacheManager.get(pageId, PAGE_SIZE_BYTES, mBuffer, 0);
      assertArrayEquals(PAGE, mBuffer);
      if (ret <= 0) {
        evicted++;
        continue;
      }
      assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(pageId, PAGE_SIZE_BYTES, mBuffer, 0));
    }
    // verifies half of the loaded pages are evicted
    assertEquals(PAGE_COUNT / 2, evicted);
    // verifies the newly added pages are cached
    for (int i = 0; i < PAGE_COUNT / 2; i++) {
      testPageCached(new PageId("1", i));
    }
  }

  private void testLoadCache() throws Exception {
    mCacheManager = LocalCacheManager.create(mConf);
    mCacheManager.put(PAGE_ID, PAGE);
    // verify reading from local cache
    testPageCached();

    mCacheManager.close();
    // creates with same configuration
    mCacheManager = LocalCacheManager.create(mConf);

    // verify reading from recovered local cache
    testPageCached();
  }

  @Test
  public void loadCacheMismatchedPageSize() throws Exception {
    testLoadCacheConfMismatch(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES * 2);
  }

  @Test
  public void loadCacheMismatchedStoreTypeRocks() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    testLoadCacheConfMismatch(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "ROCKS");
  }

  @Test
  public void loadCacheMismatchedStoreTypeLocal() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "ROCKS");
    testLoadCacheConfMismatch(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
  }

  @Test
  public void loadCacheSmallerNewCacheSizeRocks() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "ROCKS");
    testLoadCacheConfMismatch(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES / 2);
  }

  @Test
  public void loadCacheSmallerNewCacheSizeLocal() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    loadFullCache();
    mCacheManager.close();
    // creates with different configuration
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES / 2);
    mCacheManager = LocalCacheManager.create(mConf);
    CommonUtils.waitFor("async restore completed",
        () ->  mCacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    int hits = 0;
    for (int i = 0; i < PAGE_COUNT; i++) {
      if (PAGE_SIZE_BYTES
          == mCacheManager.get(new PageId("0", i), PAGE_SIZE_BYTES, mBuffer, 0)) {
        hits++;
      }
    }
    if (hits < PAGE_COUNT / 2) {
      fail(String.format("Expected at least %s hits but actually got %s hits",
          PAGE_COUNT / 2, hits));
    }
  }

  @Test
  public void loadCacheWithInvalidPageFile() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    loadFullCache();
    mCacheManager.close();
    // creates with an invalid page file stored
    String rootDir = PageStore.getStorePath(PageStoreType.LOCAL,
        mConf.get(PropertyKey.USER_CLIENT_CACHE_DIR)).toString();
    FileUtils.createFile(Paths.get(rootDir, "invalidPageFile").toString());
    mCacheManager = LocalCacheManager.create(mConf);
    assertEquals(0, mCacheManager.get(PAGE_ID, PAGE_SIZE_BYTES, mBuffer, 0));
  }

  @Test
  public void loadCacheLargerNewCacheSize() throws Exception {
    testLoadCacheConfChanged(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES * 2);
    testPageCached();
  }

  private void testLoadCacheConfChanged(PropertyKey prop, Object value) throws Exception {
    mCacheManager = LocalCacheManager.create(mConf);
    mCacheManager.put(PAGE_ID, PAGE);
    // verify reading from local cache
    testPageCached();

    mCacheManager.close();
    // creates with different configuration
    mConf.set(prop, value);
    mCacheManager = LocalCacheManager.create(mConf);
  }

  private void testLoadCacheConfMismatch(PropertyKey prop, Object value) throws Exception {
    testLoadCacheConfChanged(prop, value);
    // verify failed to read from recovered local cache
    assertEquals(0, mCacheManager.get(PAGE_ID, PAGE_SIZE_BYTES, mBuffer, 0));
  }

  private void loadFullCache() throws Exception {
    mCacheManager = LocalCacheManager.create(mConf);
    for (int i = 0; i < PAGE_COUNT; i++) {
      mCacheManager.put(new PageId("0", i), PAGE);
    }
    for (int i = 0; i < PAGE_COUNT; i++) {
      testPageCached(new PageId("0", i));
    }
  }
}
