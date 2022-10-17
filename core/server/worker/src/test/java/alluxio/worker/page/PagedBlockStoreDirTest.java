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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

public class PagedBlockStoreDirTest {
  private static final int DIR_INDEX = 0;
  private PagedBlockStoreDir mDir;
  private Path mDirPath;

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    mDirPath = mTempFolder.newFolder().toPath();
    InstancedConfiguration conf = Configuration.modifiableGlobal();
    conf.set(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(mDirPath));
    conf.set(PropertyKey.WORKER_PAGE_STORE_SIZES, ImmutableList.of(Constants.MB));
    conf.set(PropertyKey.WORKER_PAGE_STORE_TYPE, PageStoreType.LOCAL);
    PageStoreDir pageStoreDir =
        PageStoreDir.createPageStoreDir(
            new CacheEvictorOptions().setEvictorClass(FIFOCacheEvictor.class),
            PageStoreOptions.createForWorkerPageStore(conf).get(DIR_INDEX));
    mDir = new PagedBlockStoreDir(pageStoreDir, DIR_INDEX);
  }

  @Test
  public void rootPath() {
    assertEquals(mDirPath.resolve(PageStoreType.LOCAL.name()), mDir.getRootPath());
  }

  @Test
  public void dirIndex() {
    assertEquals(DIR_INDEX, mDir.getDirIndex());
  }

  @Test
  public void blockLocation() {
    BlockStoreLocation location = mDir.getLocation();
    assertEquals(PagedBlockStoreMeta.DEFAULT_TIER, location.tierAlias());
    assertEquals(PagedBlockStoreMeta.DEFAULT_MEDIUM, location.mediumType());
    assertEquals(DIR_INDEX, location.dir());
  }

  @Test
  public void numBlocks() {
    assertEquals(0, mDir.getNumBlocks());
    mDir.putPage(new PageInfo(new PageId("0", 0), 0, mDir));
    assertEquals(1, mDir.getNumBlocks());
    mDir.putPage(new PageInfo(new PageId("0", 1), 0, mDir));
    assertEquals(1, mDir.getNumBlocks());
    mDir.putPage(new PageInfo(new PageId("1", 0), 0, mDir));
    assertEquals(2, mDir.getNumBlocks());
  }

  @Test
  public void cachedBytes() {
    assertEquals(0, mDir.getCachedBytes());
    mDir.putPage(new PageInfo(new PageId("0", 0), Constants.KB, mDir));
    assertEquals(Constants.KB, mDir.getCachedBytes());
    assertEquals(Constants.KB, mDir.getBlockCachedBytes(0));
    mDir.putPage(new PageInfo(new PageId("0", 1), Constants.KB, mDir));
    assertEquals(2 * Constants.KB, mDir.getCachedBytes());
    assertEquals(2 * Constants.KB, mDir.getBlockCachedBytes(0));
    mDir.putPage(new PageInfo(new PageId("1", 0), Constants.KB, mDir));
    assertEquals(3 * Constants.KB, mDir.getCachedBytes());
    assertEquals(Constants.KB, mDir.getBlockCachedBytes(1));
  }

  @Test
  public void putPage() throws Exception {
    long blockId = 0;
    PageId pageId = new PageId(String.valueOf(blockId), 0);
    PageInfo pageInfo = new PageInfo(pageId, Constants.KB, mDir);
    mDir.putPage(pageInfo);
    assertEquals(1, mDir.getBlockCachedPages(blockId));
    assertTrue(mDir.hasFile(String.valueOf(blockId)));

    // duplicate adds are ignored
    mDir.putPage(pageInfo);
    assertEquals(1, mDir.getBlockCachedPages(blockId));
  }

  @Test
  public void deletePage() throws Exception {
    long blockId = 0;
    PageId pageId = new PageId(String.valueOf(blockId), 0);
    PageInfo pageInfo = new PageInfo(pageId, Constants.KB, mDir);
    mDir.putPage(pageInfo);
    assertEquals(1, mDir.getBlockCachedPages(blockId));
    assertEquals(Constants.KB, mDir.getBlockCachedBytes(blockId));

    long cachedBytes = mDir.deletePage(pageInfo);
    assertEquals(0, mDir.getBlockCachedPages(blockId));
    assertEquals(0, mDir.getBlockCachedBytes(blockId));
    assertEquals(mDir.getCachedBytes(), cachedBytes);

    // deleting a non-existing page is no-op
    long cachedBytes2 = mDir.deletePage(pageInfo);
    assertEquals(0, mDir.getBlockCachedPages(blockId));
    assertEquals(0, mDir.getBlockCachedBytes(blockId));
    assertEquals(cachedBytes, cachedBytes2);
  }

  @Test
  public void evictor() throws Exception {
    BlockPageEvictor evictor = mDir.getEvictor();

    long blockId = 0;
    PageId pageId0 = new PageId(String.valueOf(blockId), 0);
    PageInfo pageInfo0 = new PageInfo(pageId0, Constants.KB, mDir);
    mDir.putPage(pageInfo0);
    assertEquals(pageId0, evictor.evict());

    PageId pageId1 = new PageId(String.valueOf(blockId), 1);
    PageInfo pageInfo1 = new PageInfo(pageId1, Constants.KB, mDir);
    mDir.putPage(pageInfo1);
    mDir.deletePage(pageInfo0);
    assertEquals(pageId1, evictor.evict());
  }

  @Test
  public void addTempPage() throws Exception {
    long blockId = 0;
    final String fileId = String.valueOf(blockId);
    PageId pageId0 = new PageId(fileId, 0);
    PageInfo pageInfo0 = new PageInfo(pageId0, Constants.KB, mDir);
    mDir.putTempPage(pageInfo0);
    mDir.getPageStore().putTemporary(pageId0, new byte[Constants.KB]);
    assertEquals(0, mDir.getBlockCachedPages(blockId));
    assertFalse(mDir.hasFile(fileId));
    assertTrue(mDir.hasTempFile(fileId));

    mDir.commit(fileId);
    assertEquals(1, mDir.getBlockCachedPages(blockId));
    assertTrue(mDir.hasFile(fileId));
    assertFalse(mDir.hasTempFile(fileId));
  }

  @Test
  public void abortTempPage() throws Exception {
    long blockId = 0;
    final String fileId = String.valueOf(blockId);
    PageId pageId0 = new PageId(fileId, 0);
    PageInfo pageInfo0 = new PageInfo(pageId0, Constants.KB, mDir);
    mDir.putTempPage(pageInfo0);
    mDir.getPageStore().putTemporary(pageId0, new byte[Constants.KB]);
    assertEquals(0, mDir.getBlockCachedPages(blockId));
    assertTrue(mDir.hasTempFile(fileId));
    assertFalse(mDir.hasFile(fileId));

    mDir.abort(fileId);
    assertEquals(0, mDir.getBlockCachedPages(blockId));
    assertFalse(mDir.hasTempFile(fileId));
    assertFalse(mDir.hasFile(fileId));
  }
}
