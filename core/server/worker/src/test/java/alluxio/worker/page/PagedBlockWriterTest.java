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

package alluxio.worker.page;

import static org.junit.Assert.assertEquals;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.LocalPageStoreDir;
import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.List;

public class PagedBlockWriterTest {
  private static final int TEST_BLOCK_SIZE = 1024;
  private static final int TEST_PAGE_SIZE = 128;
  private static final long BLOCK_ID = 1L;

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private PageMetaStore mPageMetaStore;
  private CacheEvictor mEvictor;
  private LocalPageStoreOptions mPageStoreOptions;
  private PageStore mPageStore;
  private LocalPageStoreDir mPageStoreDir;
  private PagedBlockWriter mWriter;
  private String mTestFilePath;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, TEST_PAGE_SIZE);
    mTestFilePath = mFolder.newFile().getAbsolutePath();
    mPageMetaStore = new DefaultPageMetaStore();
    mPageStoreOptions = (LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0);
    mPageStore = PageStore.create(mPageStoreOptions);
    mEvictor = new FIFOCacheEvictor(mConf);
    mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
    mCacheManager =
        LocalCacheManager.create(mConf, mPageMetaStore, ImmutableList.of(mPageStoreDir));
    CommonUtils.waitFor("restore completed",
        () -> mCacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    mWriter = new PagedBlockWriter(mCacheManager, BLOCK_ID, TEST_PAGE_SIZE);
  }

  @After
  public void after() throws Exception {
    mWriter.close();
  }

  @Test
  public void appendByteBuf() throws Exception {
    ByteBuf buffer = Unpooled.wrappedBuffer(
        BufferUtils.getIncreasingByteBuffer(TEST_BLOCK_SIZE));
    buffer.markReaderIndex();
    assertEquals(TEST_BLOCK_SIZE, mWriter.append(buffer));
    buffer.resetReaderIndex();
    assertEquals(TEST_BLOCK_SIZE, mWriter.append(buffer));
    mWriter.close();
    verifyDataInCache();
  }

  @Test
  public void append() throws Exception {
    assertEquals(TEST_BLOCK_SIZE,
        mWriter.append(BufferUtils.getIncreasingByteBuffer(TEST_BLOCK_SIZE)));
    assertEquals(TEST_BLOCK_SIZE,
        mWriter.append(BufferUtils.getIncreasingByteBuffer(TEST_BLOCK_SIZE)));
    mWriter.close();
    verifyDataInCache();
  }

  private void verifyDataInCache() {
    List<PageId> pageIds =
        mCacheManager.getCachedPageIdsByFileId(String.valueOf(BLOCK_ID), TEST_BLOCK_SIZE * 2);
    assertEquals(TEST_BLOCK_SIZE * 2 / TEST_PAGE_SIZE, pageIds.size());
    byte[] dataInCache = new byte[TEST_BLOCK_SIZE * 2];
    for (int i = 0; i < pageIds.size(); i++) {
      PageId pageId = pageIds.get(i);
      mCacheManager.get(pageId, 0, TEST_PAGE_SIZE, dataInCache, i * TEST_PAGE_SIZE);
    }
    ByteBuffer result = ByteBuffer.wrap(dataInCache);
    result.position(0).limit(TEST_BLOCK_SIZE);
    BufferUtils.equalIncreasingByteBuffer(0, TEST_BLOCK_SIZE, result.slice());
    result.position(TEST_BLOCK_SIZE).limit(TEST_BLOCK_SIZE * 2);
    BufferUtils.equalIncreasingByteBuffer(0, TEST_BLOCK_SIZE, result.slice());
  }
}
