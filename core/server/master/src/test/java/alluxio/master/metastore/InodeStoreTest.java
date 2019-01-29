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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.ConfigurationBuilder;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.metastore.InodeStore.InodeStoreArgs;
import alluxio.master.metastore.InodeStore.WriteBatch;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;

import com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class InodeStoreTest {
  private static final int CACHE_SIZE = 16;

  @Parameters
  public static Iterable<Supplier<InodeStore>> parameters() throws Exception {
    InstancedConfiguration conf = new ConfigurationBuilder()
        .setProperty(PropertyKey.MASTER_METASTORE_DIR,
            AlluxioTestDirectory.createTemporaryDirectory("inode-store-test"))
        .setProperty(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, CACHE_SIZE)
        .build();
    InodeStoreArgs args = new InodeStoreArgs(new InodeLockManager(), conf);
    return Arrays.asList(
        () -> new HeapInodeStore(args),
        () -> new RocksInodeStore(args),
        () -> new CachingInodeStore(new RocksInodeStore(args), args));
  }

  private final MutableInodeDirectory mRoot = inodeDir(0, -1, "");

  private final InodeStore mStore;

  public InodeStoreTest(Supplier<InodeStore> store) {
    mStore = store.get();
  }

  @Test
  public void get() {
    mStore.writeInode(mRoot);
    assertEquals(Inode.wrap(mRoot), mStore.get(0).get());
  }

  @Test
  public void getMutable() {
    mStore.writeInode(mRoot);
    assertEquals(mRoot, mStore.getMutable(0).get());
  }

  @Test
  public void getChild() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    mStore.writeInode(mRoot);
    mStore.writeInode(child);
    mStore.addChild(mRoot.getId(), child);
    assertEquals(Inode.wrap(child), mStore.getChild(mRoot, child.getName()).get());
  }

  @Test
  public void remove() {
    mStore.writeInode(mRoot);
    mStore.remove(mRoot);
  }

  @Test
  public void removeChild() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    mStore.writeInode(mRoot);
    mStore.writeInode(child);
    mStore.addChild(mRoot.getId(), child);
    mStore.removeChild(mRoot.getId(), child.getName());
    assertFalse(mStore.getChild(mRoot, child.getName()).isPresent());
  }

  @Test
  public void updateInode() {
    mStore.writeInode(mRoot);
    MutableInodeDirectory mutableRoot = mStore.getMutable(mRoot.getId()).get().asDirectory();
    mutableRoot.setLastModificationTimeMs(163, true);
    mStore.writeInode(mutableRoot);
    assertEquals(163, mStore.get(mRoot.getId()).get().getLastModificationTimeMs());
  }

  @Test
  public void batchWrite() {
    assumeTrue(mStore.supportsBatchWrite());
    WriteBatch batch = mStore.createWriteBatch();
    MutableInodeFile child = inodeFile(1, 0, "child");
    batch.writeInode(child);
    batch.addChild(mRoot.getId(), child.getName(), child.getId());
    batch.commit();
    assertEquals(Inode.wrap(child), mStore.get(child.getId()).get());
    assertEquals(Inode.wrap(child), mStore.getChild(mRoot.getId(), child.getName()).get());

    batch = mStore.createWriteBatch();
    batch.removeInode(child.getId());
    batch.removeChild(mRoot.getId(), child.getName());
    batch.commit();
    assertEquals(Optional.empty(), mStore.get(child.getId()));
    assertEquals(Optional.empty(), mStore.getChild(mRoot.getId(), child.getName()));
  }

  @Test
  public void addRemoveAddList() {
    mStore.writeInode(mRoot);
    for (int i = 1; i < 10; i++) {
      MutableInodeFile file = inodeFile(i, 0, "file" + i);
      mStore.writeInode(file);
      mStore.addChild(mRoot.getId(), file);
    }
    assertEquals(9, Iterables.size(mStore.getChildren(mRoot)));

    for (Inode child : mStore.getChildren(mRoot)) {
      mStore.removeChild(mRoot.getId(), child.getName());
      mStore.remove(child);
    }
    for (int i = 1; i < 10; i++) {
      MutableInodeFile file = inodeFile(i, 0, "file" + i);
      mStore.writeInode(file);
      mStore.addChild(mRoot.getId(), file);
    }
    assertEquals(9, Iterables.size(mStore.getChildren(mRoot)));
  }

  @Test
  public void repeatedAddRemoveAndList() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    mStore.writeInode(child);
    mStore.addChild(mRoot.getId(), child);
    for (int i = 0; i < 3; i++) {
      mStore.removeChild(mRoot.getId(), child.getName());
      mStore.addChild(mRoot.getId(), child);
    }
    List<MutableInodeDirectory> dirs = new ArrayList<>();
    for (int i = 5; i < 5 + CACHE_SIZE; i++) {
      String childName = "child" + i;
      MutableInodeDirectory dir = inodeDir(i, 0, childName);
      dirs.add(dir);
      mStore.addChild(mRoot.getId(), dir);
      mStore.getChild(mRoot, childName);
    }
    for (MutableInodeDirectory dir : dirs) {
      mStore.removeChild(mRoot.getId(), dir.getName());
    }
    assertEquals(1, Iterables.size(mStore.getChildren(mRoot)));
  }

  @Test
  public void manyOperations() {
    mStore.writeInode(mRoot);
    MutableInodeDirectory curr = mRoot;
    List<Long> fileIds = new ArrayList<>();
    long numDirs = 100;
    // Create 100 nested directories, each containing a file.
    for (int i = 1; i < numDirs; i++) {
      MutableInodeDirectory dir = inodeDir(i, curr.getId(), "dir" + i);
      MutableInodeFile file = inodeFile(i + 1000, i, "file" + i);
      fileIds.add(file.getId());
      mStore.writeInode(dir);
      mStore.writeInode(file);
      mStore.addChild(curr.getId(), dir);
      mStore.addChild(dir.getId(), file);
      curr = dir;
    }

    // Check presence and delete files.
    for (int i = 0; i < numDirs; i++) {
      assertTrue(mStore.get(i).isPresent());
    }
    for (Long i : fileIds) {
      assertTrue(mStore.get(i).isPresent());
      Inode inode = mStore.get(i).get();
      mStore.remove(inode);
      mStore.removeChild(inode.getParentId(), inode.getName());
      assertFalse(mStore.get(i).isPresent());
      assertFalse(mStore.getChild(inode.getParentId(), inode.getName()).isPresent());
    }

    long middleDir = numDirs / 2;
    // Rename a directory
    MutableInodeDirectory dir = mStore.getMutable(middleDir).get().asDirectory();
    mStore.removeChild(middleDir - 1, "dir" + middleDir);
    mStore.addChild(mRoot.getId(), dir);
    dir.setParentId(mRoot.getId());
    mStore.writeInode(dir);

    Optional<Inode> renamed = mStore.getChild(mRoot, dir.getName());
    assertTrue(renamed.isPresent());
    assertTrue(mStore.getChild(renamed.get().asDirectory(), "dir" + (middleDir + 1)).isPresent());
    assertEquals(0,
        Iterables.size(mStore.getChildren(mStore.get(middleDir - 1).get().asDirectory())));
  }

  private static MutableInodeDirectory inodeDir(long id, long parentId, String name) {
    return MutableInodeDirectory.create(id, parentId, name, CreateDirectoryContext.defaults());
  }

  private static MutableInodeFile inodeFile(long containerId, long parentId, String name) {
    return MutableInodeFile.create(containerId, parentId, name, 0, CreateFileContext.defaults());
  }
}
