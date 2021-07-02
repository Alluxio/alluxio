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

package alluxio.master.metastore.caching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.heap.HeapInodeStore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CachingInodeStoreMockedBackingStoreTest {
  private static final long CACHE_SIZE = 20;
  private static final long TEST_INODE_ID = 5;
  private static final MutableInodeDirectory TEST_INODE_DIR =
      MutableInodeDirectory.create(TEST_INODE_ID, 0, "name", CreateDirectoryContext.defaults());

  private InodeStore mBackingStore;
  private CachingInodeStore mStore;

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(
      ImmutableMap.of(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, Long.toString(CACHE_SIZE),
          PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE, "5"),
      ServerConfiguration.global());

  @Before
  public void before() {
    mBackingStore = spy(new HeapInodeStore());
    mStore = new CachingInodeStore(mBackingStore, new InodeLockManager());
    mStore.writeNewInode(TEST_INODE_DIR);
  }

  @After
  public void after() {
    mStore.close();
  }

  @Test
  public void cacheGetMutable() {
    for (int i = 0; i < 10; i++) {
      assertEquals(TEST_INODE_DIR, mStore.getMutable(TEST_INODE_ID).get());
    }

    verifyNoBackingStoreReads();
  }

  @Test
  public void cacheGet() {
    Inode testInode = Inode.wrap(TEST_INODE_DIR);
    for (int i = 0; i < CACHE_SIZE / 2; i++) {
      assertEquals(testInode, mStore.get(TEST_INODE_ID).get());
    }

    verifyNoBackingStoreReads();
  }

  @Test
  public void cacheGetMany() {
    for (long inodeId = 1; inodeId < CACHE_SIZE * 2; inodeId++) {
      createInodeDir(inodeId, 0);
    }
    for (int i = 0; i < 30; i++) {
      for (long inodeId = 1; inodeId < CACHE_SIZE * 2; inodeId++) {
        assertTrue(mStore.get(inodeId).isPresent());
      }
    }
    // The workload is read-only, so we shouldn't need to write each inode to the backing store more
    // than once.
    verify(mBackingStore, atMost((int) CACHE_SIZE * 2 + 1)).writeInode(any());
  }

  @Test
  public void removeInodeStaysRemoved() {
    mStore.remove(TEST_INODE_DIR);

    assertEquals(Optional.empty(), mStore.get(TEST_INODE_DIR.getId()));
  }

  @Test
  public void reflectWrite() {
    MutableInodeDirectory updated = MutableInodeDirectory.create(TEST_INODE_ID, 10, "newName",
        CreateDirectoryContext.defaults());
    mStore.writeInode(updated);

    assertEquals("newName", mStore.get(TEST_INODE_ID).get().getName());
  }

  @Test
  public void cacheGetChild() {
    MutableInodeFile child =
        MutableInodeFile.create(0, TEST_INODE_ID, "child", 0, CreateFileContext.defaults());
    mStore.writeInode(child);
    mStore.addChild(TEST_INODE_ID, child);
    for (int i = 0; i < 10; i++) {
      assertTrue(mStore.getChild(TEST_INODE_DIR, "child").isPresent());
    }

    verifyNoBackingStoreReads();
  }

  @Test
  public void listChildrenOverCacheSize() {
    for (long inodeId = 10; inodeId < 10 + CACHE_SIZE * 2; inodeId++) {
      MutableInodeDirectory dir = createInodeDir(inodeId, 0);
      mStore.addChild(0, dir);
    }

    assertEquals(CACHE_SIZE * 2, Iterables.size(mStore.getChildren(0L)));
  }

  @Test
  public void cacheGetChildMany() {
    for (long inodeId = 1; inodeId < CACHE_SIZE * 2; inodeId++) {
      MutableInodeFile child =
          MutableInodeFile.create(0, 0, "child" + inodeId, 0, CreateFileContext.defaults());
      mStore.writeInode(child);
      mStore.addChild(0, child);
    }
    for (int i = 0; i < 1000; i++) {
      for (long inodeId = 1; inodeId < CACHE_SIZE * 2; inodeId++) {
        assertTrue(mStore.getChild(0L, "child" + inodeId).isPresent());
      }
    }

    // The workload is read-only, so we shouldn't need to write each edge to the backing store more
    // than once.
    verify(mBackingStore, atMost((int) CACHE_SIZE * 2)).addChild(anyLong(), any(), anyLong());
  }

  @Test
  public void cacheGetChildrenInodeLookups() {
    List<Inode> children = new ArrayList<>();
    for (int id = 100; id < 110; id++) {
      MutableInodeFile child =
          MutableInodeFile.create(id, TEST_INODE_ID, "child" + id, 0, CreateFileContext.defaults());
      children.add(Inode.wrap(child));
      mStore.writeNewInode(child);
      mStore.addChild(TEST_INODE_ID, child);
    }
    assertEquals(10, Iterables.size(mStore.getChildren(TEST_INODE_DIR)));

    verifyNoBackingStoreReads();
  }

  @Test
  public void eviction() {
    for (int id = 100; id < 100 + CACHE_SIZE * 2; id++) {
      MutableInodeFile child =
          MutableInodeFile.create(id, TEST_INODE_ID, "child" + id, 0, CreateFileContext.defaults());
      mStore.writeNewInode(child);
      mStore.addChild(TEST_INODE_ID, child);
    }
    for (int id = 100; id < 100 + CACHE_SIZE * 2; id++) {
      assertTrue(mStore.getChild(TEST_INODE_DIR, "child" + id).isPresent());
    }
    verify(mBackingStore, Mockito.atLeastOnce()).getMutable(anyLong(), any(ReadOption.class));
  }

  @Test
  public void edgeIndexTest() throws Exception {
    // Run many concurrent operations, then check that the edge cache's indices are accurate.
    long endTimeMs = System.currentTimeMillis() + 200;
    ThreadLocalRandom random = ThreadLocalRandom.current();
    List<MutableInodeDirectory> dirs = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      MutableInodeDirectory dir = createInodeDir(i, 0);
      dirs.add(dir);
      mStore.addChild(TEST_INODE_ID, dir);
    }

    AtomicInteger operations = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(10);
    int numThreads = 10;
    executor.invokeAll(Collections.nCopies(numThreads, () -> {
      while (operations.get() < 10_000 || System.currentTimeMillis() < endTimeMs) {
        // Sometimes add, sometimes delete.
        if (random.nextBoolean()) {
          MutableInodeDirectory dir =
              createInodeDir(random.nextLong(10, 15), random.nextLong(1, 5));
          mStore.addChild(dir.getParentId(), dir);
        } else {
          mStore.removeChild(dirs.get(random.nextInt(dirs.size())).getId(),
              Long.toString(random.nextLong(10, 15)));
        }
        operations.incrementAndGet();
        assertTrue(mStore.mEdgeCache.mMap.size() <= CACHE_SIZE + numThreads);
      }
      return null;
    }));
    alluxio.util.CommonUtils.waitFor("eviction thread to finish",
        () -> mStore.mEdgeCache.mEvictionThread.mIsSleeping);
    mStore.mEdgeCache.verifyIndices();
  }

  @Test
  public void listingCacheManyDirsEviction() throws Exception {
    for (int i = 1; i < CACHE_SIZE * 3; i++) {
      createInodeDir(i, TEST_INODE_ID);
    }
    assertFalse(mStore.mListingCache.getCachedChildIds(TEST_INODE_ID).isPresent());
  }

  @Test
  public void listingCacheBigDirEviction() throws Exception {
    MutableInodeDirectory bigDir = createInodeDir(1, 0);
    long dirSize = CACHE_SIZE;
    for (int i = 10; i < 10 + dirSize; i++) {
      mStore.addChild(bigDir.getId(), createInodeDir(i, bigDir.getId()));
    }
    // Cache the large directory
    assertEquals(dirSize, Iterables.size(mStore.getChildIds(bigDir.getId())));
    // Perform another operation to trigger eviction
    mStore.addChild(bigDir.getId(), createInodeDir(10000, bigDir.getId()));
    assertFalse(mStore.mListingCache.getCachedChildIds(TEST_INODE_ID).isPresent());
  }

  @Test(timeout = 10000)
  public void listingCacheAddRemoveEdges() throws Exception {
    // Perform operations including adding and removing many files within a directory. This test has
    // rooted out some bugs related to cache weight tracking.
    MutableInodeDirectory bigDir = createInodeDir(1, 0);
    mStore.writeNewInode(bigDir);
    for (int i = 1000; i < 1000 + CACHE_SIZE; i++) {
      MutableInodeDirectory subDir = createInodeDir(i, bigDir.getId());
      mStore.addChild(bigDir.getId(), subDir);
      mStore.removeChild(bigDir.getId(), subDir.getName());
    }

    List<MutableInodeDirectory> inodes = new ArrayList<>();
    for (int i = 10; i < 10 + (CACHE_SIZE / 2); i++) {
      MutableInodeDirectory otherDir = createInodeDir(i, 0);
      inodes.add(otherDir);
      mStore.writeNewInode(otherDir);
    }
    for (MutableInodeDirectory inode : inodes) {
      for (int i = 0; i < 10; i++) {
        assertEquals(0, Iterables.size(mStore.getChildIds(inode.getId())));
      }
      verify(mBackingStore, times(0)).getChildIds(inode.getId());
    }
  }

  @Test
  public void flushToBackingStore() throws Exception {
    for (long inodeId = 10; inodeId < 10 + CACHE_SIZE / 2; inodeId++) {
      MutableInodeDirectory dir = createInodeDir(inodeId, 0);
      mStore.addChild(0, dir);
    }
    assertEquals(0, Iterables.size(mBackingStore.getChildren(0L)));
    mStore.mEdgeCache.flush();
    mStore.mInodeCache.flush();
    assertEquals(CACHE_SIZE / 2, Iterables.size(mBackingStore.getChildren(0L)));
  }

  private MutableInodeDirectory createInodeDir(long id, long parentId) {
    MutableInodeDirectory dir = MutableInodeDirectory.create(id, parentId, Long.toString(id),
        CreateDirectoryContext.defaults());
    mStore.writeNewInode(dir);
    return dir;
  }

  @Test
  public void backupRestore() throws Exception {
    MutableInodeDirectory child = createInodeDir(10, 0);
    mStore.writeNewInode(child);
    mStore.addChild(0, child);
    mStore
        .writeInode(MutableInodeDirectory.create(1, 1, "blah", CreateDirectoryContext.defaults()));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    mStore.writeToCheckpoint(baos);
    mStore.restoreFromCheckpoint(
        new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())));
    assertEquals(child.getName(), mStore.get(child.getId()).get().getName());
    assertEquals(child.getId(), mStore.getChild(0L, child.getName()).get().getId());
  }

  @Test
  public void skipCache() throws Exception {
    assertEquals(1, mStore.mInodeCache.getCacheMap().size());
    mStore.mInodeCache.flush();
    mStore.mInodeCache.clear();
    assertEquals(0, mStore.mInodeCache.getCacheMap().size());
    assertEquals(Inode.wrap(TEST_INODE_DIR),
        mStore.get(TEST_INODE_ID, ReadOption.newBuilder().setSkipCache(true).build()).get());
    assertEquals(0, mStore.mInodeCache.getCacheMap().size());
  }

  private void verifyNoBackingStoreReads() {
    verify(mBackingStore, times(0)).getChild(anyLong(), anyString());
    verify(mBackingStore, times(0)).getChildId(anyLong(), anyString());
    verify(mBackingStore, times(0)).getChildren(anyLong());
    verify(mBackingStore, times(0)).getChildIds(anyLong());
    verify(mBackingStore, times(0)).get(anyLong());
    verify(mBackingStore, times(0)).getMutable(anyLong());
  }
}
