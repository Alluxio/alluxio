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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
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
import java.io.ByteArrayOutputStream;
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
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(false);
    when(mPageStore.size()).thenReturn(0);
    mCacheManager.put(0L, 0L, data);
    verify(mMetaStore).addPage(0L, 0L);
    verify(mPageStore).put(0L, 0L, data);
  }

  @Test
  public void putExist() throws Exception {
    byte[] data = BufferUtils.getIncreasingByteArray(1);
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(true);
    when(mPageStore.size()).thenReturn(1);
    mCacheManager.put(0L, 0L, data);
    verify(mMetaStore, never()).addPage(0L, 0L);
    verify(mPageStore).delete(0L, 0L);
    verify(mPageStore).put(0L, 0L, data);
  }

  @Test
  public void putEvict() throws Exception {
    byte[] data = BufferUtils.getIncreasingByteArray(1);
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(false);
    when(mMetaStore.hasPage(1L, 1L)).thenReturn(true);
    when(mPageStore.size()).thenReturn((int) mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
    when(mEvictor.evict()).thenReturn(new Pair(1L, 1L));
    mCacheManager.put(0L, 0L, data);
    verify(mMetaStore).addPage(0L, 0L);
    verify(mMetaStore).removePage(1L, 1L);
    verify(mPageStore).delete(1L, 1L);
    verify(mPageStore).put(0L, 0L, data);
  }

  @Test
  public void getExist() throws Exception {
    ReadableByteChannel channel = mock(ReadableByteChannel.class);
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(true);
    when(mPageStore.size()).thenReturn((int) mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
    when(mPageStore.get(0L, 0L)).thenReturn(channel);
    ReadableByteChannel ret = mCacheManager.get(0L, 0L);
    Assert.assertEquals(channel, ret);
    verify(mEvictor).updateOnGet(0L, 0L);
    verify(mPageStore).get(0L, 0L);
  }

  @Test
  public void getNotExist() throws Exception {
    ReadableByteChannel channel = mock(ReadableByteChannel.class);
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(false);
    mThrown.expect(PageNotFoundException.class);
    try {
      ReadableByteChannel ret = mCacheManager.get(0L, 0L);
    } finally {
      verify(mEvictor, never()).updateOnGet(0L, 0L);
      verify(mPageStore, never()).get(0L, 0L);
    }
  }

  @Test
  public void getOffset() throws Exception {
    long pageSize = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    ByteBuffer buf = BufferUtils.getIncreasingByteBuffer((int) pageSize);
    ByteBuffer retBuf = ByteBuffer.allocate((int) pageSize);
    try (ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(buf.array()))) {
      when(mMetaStore.hasPage(0L, 0L)).thenReturn(true);
      when(mPageStore.size()).thenReturn((int) mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE));
      when(mPageStore.get(0L, 0L)).thenReturn(channel);
      ByteArrayOutputStream dst = new ByteArrayOutputStream();
      int size = 0;
      try (ReadableByteChannel ret = mCacheManager.get(0L, 0L, 1, 2)) {
        Assert.assertEquals(2, ret.read(retBuf));
      }
    }
    retBuf.flip();
    verify(mEvictor).updateOnGet(0L, 0L);
    verify(mPageStore).get(eq(0L), eq(0L));
    Assert.assertArrayEquals(new byte[] {1, 2}, BufferUtils.newByteArrayFromByteBuffer(retBuf));
  }

  @Test
  public void deleteExist() throws Exception {
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(true);
    mCacheManager.delete(0L, 0L);
    verify(mMetaStore).removePage(0L, 0L);
    verify(mPageStore).delete(0L, 0L);
  }

  @Test
  public void deleteNotExist() throws Exception {
    when(mMetaStore.hasPage(0L, 0L)).thenReturn(false);
    doThrow(new PageNotFoundException("test")).when(mMetaStore).removePage(0L, 0L);
    doThrow(new PageNotFoundException("test")).when(mPageStore).delete(0L, 0L);
    mThrown.expect(PageNotFoundException.class);
    try {
      mCacheManager.delete(0L, 0L);
    } finally {
      verify(mMetaStore).removePage(0L, 0L);
    }
  }
}
