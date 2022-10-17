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
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.LocalCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.ByteArrayTargetBuffer;
import alluxio.client.file.cache.store.LocalPageStoreDir;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class PagedBlockWriterTest {
  private static final long BLOCK_ID = 1L;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        /*file_length, chuck_size, page_size*/
        {2048, 1024, 128},
        {2049, 1024, 128},
        {2048, 1023, 128},
        {2048, 1024, 129},
    });
  }

  @Parameterized.Parameter
  public int mFileLength;

  @Parameterized.Parameter(1)
  public int mChunkSize;

  @Parameterized.Parameter(2)
  public int mPageSize;

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private PageMetaStore mPageMetaStore;
  private CacheEvictor mEvictor;
  private CacheManagerOptions mCachemanagerOptions;
  private PageStoreOptions mPageStoreOptions;
  private PageStore mPageStore;
  private LocalPageStoreDir mPageStoreDir;
  private PagedBlockWriter mWriter;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, mPageSize);
    mConf.set(PropertyKey.WORKER_PAGE_STORE_CACHE_DIRS, ImmutableList.of(
        AlluxioTestDirectory.createTemporaryDirectory("page_store").getAbsolutePath()));
    mCachemanagerOptions = CacheManagerOptions.createForWorker(mConf);
    mPageStoreOptions = mCachemanagerOptions.getPageStoreOptions().get(0);
    mPageStore = PageStore.create(mPageStoreOptions);
    mEvictor = new FIFOCacheEvictor(mCachemanagerOptions.getCacheEvictorOptions());
    mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
    mPageStoreDir.reset();
    mPageMetaStore = new DefaultPageMetaStore(ImmutableList.of(mPageStoreDir));
    mCacheManager =
        LocalCacheManager.create(mCachemanagerOptions, mPageMetaStore);
    CommonUtils.waitFor("restore completed",
        () -> mCacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    mWriter = new PagedBlockWriter(mCacheManager, BLOCK_ID, mPageSize);
  }

  @After
  public void after() throws Exception {
    mWriter.close();
  }

  @Test
  public void appendByteBuf() throws Exception {
    for (int offset = 0; offset < mFileLength; offset += mChunkSize) {
      int bytesToWrite = Math.min(mChunkSize, mFileLength - offset);
      ByteBuf buffer = Unpooled.wrappedBuffer(
          BufferUtils.getIncreasingByteBuffer(bytesToWrite));
      assertEquals(bytesToWrite, mWriter.append(buffer));
    }
    mWriter.close();
    mPageMetaStore.commitFile(BlockPageId.tempFileIdOf(BLOCK_ID),
        BlockPageId.fileIdOf(BLOCK_ID, mFileLength));
    mPageStoreDir.commit(BlockPageId.tempFileIdOf(BLOCK_ID),
        BlockPageId.fileIdOf(BLOCK_ID, mFileLength));
    verifyDataInCache();
  }

  @Test
  public void append() throws Exception {
    for (int offset = 0; offset < mFileLength; offset += mChunkSize) {
      int bytesToWrite = Math.min(mChunkSize, mFileLength - offset);
      ByteBuffer buffer =
          BufferUtils.getIncreasingByteBuffer(bytesToWrite);
      assertEquals(bytesToWrite, mWriter.append(buffer));
    }
    mWriter.close();
    mPageMetaStore.commitFile(BlockPageId.tempFileIdOf(BLOCK_ID),
        BlockPageId.fileIdOf(BLOCK_ID, mFileLength));
    mPageStoreDir.commit(BlockPageId.tempFileIdOf(BLOCK_ID),
        BlockPageId.fileIdOf(BLOCK_ID, mFileLength));
    verifyDataInCache();
  }

  private void verifyDataInCache() {
    List<PageId> pageIds =
        mCacheManager.getCachedPageIdsByFileId(
            BlockPageId.fileIdOf(BLOCK_ID, mFileLength), mFileLength);
    assertEquals((int) Math.ceil((double) mFileLength / mPageSize), pageIds.size());
    byte[] dataInCache = new byte[mFileLength];
    for (int i = 0; i < pageIds.size(); i++) {
      PageId pageId = pageIds.get(i);
      mCacheManager.get(pageId, 0, Math.min(mPageSize, mFileLength - i * mPageSize),
          new ByteArrayTargetBuffer(dataInCache, i * mPageSize),
          CacheContext.defaults().setTemporary(false));
    }
    for (int offset = 0; offset < mFileLength; offset += mChunkSize) {
      int chunkLength = Math.min(mChunkSize, mFileLength - offset);
      byte[] chunk = new byte[chunkLength];
      System.arraycopy(dataInCache, offset, chunk, 0, chunkLength);
      assertTrue(
          BufferUtils.equalIncreasingByteArray(chunkLength, chunk));
    }
  }
}
