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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.io.BufferUtils;

import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public final class LocalCacheManagerIntegrationTest extends BaseIntegrationTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int PAGE_COUNT = 32;
  private static final int CACHE_SIZE_BYTES = PAGE_COUNT * PAGE_SIZE_BYTES;
  private static final PageId PAGE_ID = new PageId(0L, 0L);
  private static final byte[] PAGE = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf;

  @Before
  public void before() throws Exception {
    mConf = new InstancedConfiguration(new AlluxioProperties());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_LOCAL_CACHE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIR, mTemp.getRoot().getPath());
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
    ReadableByteChannel channel = mCacheManager.get(pageId);
    assertNotNull(channel);
    try (InputStream stream = Channels.newInputStream(channel)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
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
      mCacheManager.put(new PageId(1, i), PAGE);
    }
    int evicted = 0;
    for (int i = 0; i < PAGE_COUNT; i++) {
      ReadableByteChannel channel = mCacheManager.get(new PageId(0, i));
      if (channel == null) {
        evicted++;
        continue;
      }
      try (InputStream stream = Channels.newInputStream(channel)) {
        assertTrue(BufferUtils.equalIncreasingByteArray(
            PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
      }
    }
    // verifies half of the loaded pages are evicted
    assertEquals(PAGE_COUNT / 2, evicted);
    // verifies the newly added pages are cached
    for (int i = 0; i < PAGE_COUNT / 2; i++) {
      testPageCached(new PageId(1, i));
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
    try (ReadableByteChannel channel = mCacheManager.get(PAGE_ID)) {
      assertNull(channel);
    }
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
    try (ReadableByteChannel channel = mCacheManager.get(PAGE_ID)) {
      assertNull(channel);
    }
  }

  private void loadFullCache() throws Exception {
    mCacheManager = LocalCacheManager.create(mConf);
    for (int i = 0; i < PAGE_COUNT; i++) {
      mCacheManager.put(new PageId(0, i), PAGE);
    }
    for (int i = 0; i < PAGE_COUNT; i++) {
      testPageCached(new PageId(0, i));
    }
  }
}
