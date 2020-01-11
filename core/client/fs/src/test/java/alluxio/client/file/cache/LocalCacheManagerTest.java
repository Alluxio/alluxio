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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Tests for the {@link LocalCacheManager} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class})
public final class LocalCacheManagerTest {
  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemContext mFileContext;
  private MetaStore mMetaStore;
  private PageStore mPageStore;
  private CacheEvictor mEvictor;
  private final PageId mPage1 = new PageId(0L, 0L);
  private final PageId mPage2 = new PageId(1L, 1L);

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the mocks.
   */
  @Before
  public void before() throws Exception {
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getPathConf(any())).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    mMetaStore = mock(MetaStore.class);
    mPageStore = mock(PageStore.class);
    mEvictor = mock(CacheEvictor.class);
    mCacheManager = new LocalCacheManager(mFileContext, mMetaStore, mPageStore, mEvictor);
  }

  @Test
  public void putNew() throws Exception {
    byte[] data = BufferUtils.getIncreasingByteArray(1);
    when(mMetaStore.hasPage(mPage1)).thenReturn(false);
    when(mPageStore.size()).thenReturn(0);
    mCacheManager.put(mPage1, data);
    verify(mMetaStore).addPage(mPage1);
    verify(mPageStore).put(mPage1, data);
  }

  @Test
  public void putExist() throws Exception {
    byte[] data = BufferUtils.getIncreasingByteArray(1);
    when(mMetaStore.hasPage(mPage1)).thenReturn(true);
    when(mPageStore.size()).thenReturn(1);
    mCacheManager.put(mPage1, data);
    verify(mMetaStore, never()).addPage(mPage1);
    verify(mPageStore).delete(mPage1);
    verify(mPageStore).put(mPage1, data);
  }

  @Test
  public void putEvict() throws Exception {
    byte[] data = BufferUtils.getIncreasingByteArray(1);
    when(mMetaStore.hasPage(mPage1)).thenReturn(false);
    when(mMetaStore.hasPage(mPage2)).thenReturn(true);
    when(mPageStore.size()).thenReturn((int) mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
    when(mEvictor.evict()).thenReturn(mPage2);
    mCacheManager.put(mPage1, data);
    verify(mMetaStore).addPage(mPage1);
    verify(mMetaStore).removePage(mPage2);
    verify(mPageStore).delete(mPage2);
    verify(mPageStore).put(mPage1, data);
  }

  @Test
  public void getExist() throws Exception {
    ReadableByteChannel channel = mock(ReadableByteChannel.class);
    when(mMetaStore.hasPage(mPage1)).thenReturn(true);
    when(mPageStore.size()).thenReturn((int) mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
    when(mPageStore.get(mPage1)).thenReturn(channel);
    ReadableByteChannel ret = mCacheManager.get(mPage1);
    Assert.assertEquals(channel, ret);
    verify(mEvictor).updateOnGet(mPage1);
    verify(mPageStore).get(mPage1);
  }

  @Test
  public void getNotExist() throws Exception {
    when(mMetaStore.hasPage(mPage1)).thenReturn(false);
    ReadableByteChannel ret = mCacheManager.get(mPage1);
    Assert.assertNull(ret);
    verify(mEvictor, never()).updateOnGet(mPage1);
    verify(mPageStore, never()).get(mPage1);
  }

  @Test
  public void getOffset() throws Exception {
    long pageSize = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer((int) pageSize);
    ByteBuffer retBuf = ByteBuffer.allocate((int) pageSize);
    try (ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(buf.array()))) {
      when(mMetaStore.hasPage(mPage1)).thenReturn(true);
      when(mPageStore.size()).thenReturn((int) mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
      when(mPageStore.get(mPage1)).thenReturn(channel);
      try (ReadableByteChannel ret = mCacheManager.get(mPage1, 1)) {
        Assert.assertEquals(pageSize - 1, ret.read(retBuf));
      }
    }
    retBuf.flip();
    verify(mEvictor).updateOnGet(mPage1);
    verify(mPageStore).get(mPage1);
    Assert.assertTrue(BufferUtils.equalIncreasingByteBuffer(1, (int) pageSize - 1, retBuf));
  }

  @Test
  public void deleteExist() throws Exception {
    when(mMetaStore.hasPage(mPage1)).thenReturn(true);
    mCacheManager.delete(mPage1);
    verify(mMetaStore).removePage(mPage1);
    verify(mPageStore).delete(mPage1);
  }

  @Test
  public void deleteNotExist() throws Exception {
    when(mMetaStore.hasPage(mPage1)).thenReturn(false);
    doThrow(new PageNotFoundException("test")).when(mMetaStore).removePage(mPage1);
    doThrow(new PageNotFoundException("test")).when(mPageStore).delete(mPage1);
    mThrown.expect(PageNotFoundException.class);
    try {
      mCacheManager.delete(mPage1);
    } finally {
      verify(mMetaStore).removePage(mPage1);
    }
  }
}
