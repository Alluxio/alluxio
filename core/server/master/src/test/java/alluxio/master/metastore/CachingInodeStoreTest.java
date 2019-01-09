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

package alluxio.master.metastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import alluxio.PropertyKey;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.metastore.java.HeapInodeStore;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CachingInodeStoreTest {

  private static final long CACHE_SIZE = 20;
  private static final long TEST_INODE_ID = 5;
  private static final MutableInodeDirectory TEST_INODE_DIR =
      MutableInodeDirectory.create(TEST_INODE_ID, 0, "name", CreateDirectoryOptions.defaults());

  private InodeStore mBackingStore;
  private CachingInodeStore mStore;

  @Before
  public void before() {
    mBackingStore = spy(new HeapInodeStore());
    mStore = new CachingInodeStore(mBackingStore, InstancedConfiguration.newBuilder()
        .setProperty(PropertyKey.MASTER_METASTORE_INODE_CACHE_SIZE, CACHE_SIZE).build());
    mStore.writeInode(TEST_INODE_DIR);
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
    for (int i = 0; i < 10; i++) {
      assertEquals(testInode, mStore.get(TEST_INODE_ID).get());
    }

    verifyNoBackingStoreReads();
  }

  @Test
  public void removeInodeStaysRemoved() {
    mStore.remove(TEST_INODE_DIR);

    assertEquals(Optional.empty(), mStore.get(TEST_INODE_DIR.getId()));
  }

  @Test
  public void reflectWrite() {
    MutableInodeDirectory updated = MutableInodeDirectory.create(TEST_INODE_ID, 10, "newName",
        CreateDirectoryOptions.defaults());
    mStore.writeInode(updated);

    assertEquals("newName", mStore.get(TEST_INODE_ID).get().getName());
  }

  @Test
  public void cacheGetChild() {
    MutableInodeFile child =
        MutableInodeFile.create(0, TEST_INODE_ID, "child", 0, CreateFileOptions.defaults());
    mStore.writeInode(child);
    mStore.addChild(TEST_INODE_ID, child);
    for (int i = 0; i < 10; i++) {
      assertTrue(mStore.getChild(TEST_INODE_DIR, "child").isPresent());
    }

    verifyNoBackingStoreReads();
  }

  @Test
  public void cacheGetChildrenInodeLookups() {
    List<Inode> children = new ArrayList<>();
    for (int id = 100; id < 110; id++) {
      MutableInodeFile child =
          MutableInodeFile.create(id, TEST_INODE_ID, "child" + id, 0, CreateFileOptions.defaults());
      children.add(Inode.wrap(child));
      mStore.writeInode(child);
      mStore.addChild(TEST_INODE_ID, child);
    }
    assertEquals(10, Iterables.size(mStore.getChildren(TEST_INODE_DIR)));

    verifyNoBackingStoreReads();
  }

  @Test
  public void eviction() {
    for (int id = 100; id < 100 + CACHE_SIZE * 2; id++) {
      MutableInodeFile child =
          MutableInodeFile.create(id, TEST_INODE_ID, "child" + id, 0, CreateFileOptions.defaults());
      mStore.writeInode(child);
      mStore.addChild(TEST_INODE_ID, child);
    }
    for (int id = 100; id < 100 + CACHE_SIZE * 2; id++) {
      assertTrue(mStore.getChild(TEST_INODE_DIR, "child" + id).isPresent());
    }
    verify(mBackingStore, Mockito.atLeastOnce()).getMutable(anyLong());
  }

  private void verifyNoBackingStoreReads() {
    verify(mBackingStore, Mockito.times(0)).getChild(any(), any());
    verify(mBackingStore, Mockito.times(0)).getChildId(any(), any());
    verify(mBackingStore, Mockito.times(0)).get(anyLong());
    verify(mBackingStore, Mockito.times(0)).getMutable(anyLong());
  }
}
