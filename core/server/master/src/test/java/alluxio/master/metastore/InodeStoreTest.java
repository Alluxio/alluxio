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

import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.metastore.InodeStore.WriteBatch;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.resource.CloseableIterator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@RunWith(Parameterized.class)
public class InodeStoreTest extends InodeStoreTestBase {
  public InodeStoreTest(Function<InodeLockManager, InodeStore> store) {
    super(store);
  }

  @Test
  public void rocksConfigFile() throws Exception {
    assumeTrue(mStore instanceof RocksInodeStore || mStore instanceof CachingInodeStore);
    // close the store first because we want to reopen it with the new config
    mStore.close();
    try (AutoCloseable ignored = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.ROCKS_INODE_CONF_FILE, sDir + CONF_NAME);
      }
    }, Configuration.modifiableGlobal()).toResource()) {
      before();
      writeInode(mRoot);
      assertEquals(Inode.wrap(mRoot), mStore.get(0).get());
    }
  }

  @Test
  public void rocksInvalidConfigFile() throws Exception {
    assumeTrue(mStore instanceof RocksInodeStore || mStore instanceof CachingInodeStore);
    // close the store first because we want to reopen it with the new config
    mStore.close();
    // write an invalid config
    String path = sDir + CONF_NAME + "invalid";
    File confFile = new File(path);
    writeStringToFile(confFile, "Invalid config", (Charset) null);

    try (AutoCloseable ignored = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.ROCKS_INODE_CONF_FILE, path);
      }
    }, Configuration.modifiableGlobal()).toResource()) {
      RuntimeException exception = assertThrows(RuntimeException.class, this::before);
      assertEquals(RocksDBException.class, exception.getCause().getClass());
    }
  }

  @Test
  public void get() {
    writeInode(mRoot);
    assertEquals(Inode.wrap(mRoot), mStore.get(0).get());
  }

  @Test
  public void getMutable() {
    writeInode(mRoot);
    assertEquals(mRoot, mStore.getMutable(0).get());
  }

  @Test
  public void getChild() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    writeInode(mRoot);
    writeInode(child);
    writeEdge(mRoot, child);
    assertEquals(Inode.wrap(child), mStore.getChild(mRoot, child.getName()).get());
  }

  @Test
  public void remove() {
    writeInode(mRoot);
    removeInode(mRoot);
  }

  @Test
  public void removeChild() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    writeInode(mRoot);
    writeInode(child);
    writeEdge(mRoot, child);
    removeParentEdge(child);
    assertFalse(mStore.getChild(mRoot, child.getName()).isPresent());
  }

  @Test
  public void updateInode() {
    writeInode(mRoot);
    MutableInodeDirectory mutableRoot = mStore.getMutable(mRoot.getId()).get().asDirectory();
    mutableRoot.setLastModificationTimeMs(163, true);
    writeInode(mutableRoot);
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
    writeInode(mRoot);
    for (int i = 1; i < 10; i++) {
      MutableInodeFile file = inodeFile(i, 0, "file" + i);
      writeInode(file);
      writeEdge(mRoot, file);
    }
    assertEquals(9, CloseableIterator.size(mStore.getChildren(mRoot)));

    try (CloseableIterator<? extends Inode> it = mStore.getChildren(mRoot)) {
      while (it.hasNext()) {
        Inode child = it.next();
        MutableInode<?> childMut = mStore.getMutable(child.getId()).get();
        removeParentEdge(childMut);
        removeInode(childMut);
      }
    }
    for (int i = 1; i < 10; i++) {
      MutableInodeFile file = inodeFile(i, 0, "file" + i);
      writeInode(file);
      writeEdge(mRoot, file);
    }
    assertEquals(9, CloseableIterator.size(mStore.getChildren(mRoot)));
  }

  @Test
  public void repeatedAddRemoveAndList() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    writeInode(mRoot);
    writeInode(child);
    writeEdge(mRoot, child);
    for (int i = 0; i < 3; i++) {
      removeParentEdge(child);
      writeEdge(mRoot, child);
    }
    List<MutableInodeDirectory> dirs = new ArrayList<>();
    for (int i = 5; i < 5 + CACHE_SIZE; i++) {
      String childName = "child" + i;
      MutableInodeDirectory dir = inodeDir(i, 0, childName);
      dirs.add(dir);
      writeInode(dir);
      writeEdge(mRoot, dir);
      mStore.getChild(mRoot, childName);
    }
    for (MutableInodeDirectory dir : dirs) {
      removeParentEdge(dir);
    }
    assertEquals(1, CloseableIterator.size(mStore.getChildren(mRoot)));
  }

  @Test
  public void manyOperations() {
    writeInode(mRoot);
    MutableInodeDirectory curr = mRoot;
    List<Long> fileIds = new ArrayList<>();
    long numDirs = 100;
    // Create 100 nested directories, each containing a file.
    for (int i = 1; i < numDirs; i++) {
      MutableInodeDirectory dir = inodeDir(i, curr.getId(), "dir" + i);
      MutableInodeFile file = inodeFile(i + 1000, i, "file" + i);
      fileIds.add(file.getId());
      writeInode(dir);
      writeInode(file);
      writeEdge(curr, dir);
      writeEdge(dir, file);
      curr = dir;
    }

    // Check presence and delete files.
    for (int i = 0; i < numDirs; i++) {
      assertTrue(mStore.get(i).isPresent());
    }
    for (Long i : fileIds) {
      assertTrue(mStore.get(i).isPresent());
      Inode inode = mStore.get(i).get();
      removeInode(inode);
      removeParentEdge(inode);
      assertFalse(mStore.get(i).isPresent());
      assertFalse(mStore.getChild(inode.getParentId(), inode.getName()).isPresent());
    }

    long middleDir = numDirs / 2;
    // Rename a directory
    MutableInodeDirectory dir = mStore.getMutable(middleDir).get().asDirectory();
    removeParentEdge(dir);
    writeEdge(mRoot, dir);
    dir.setParentId(mRoot.getId());
    writeInode(dir);

    Optional<Inode> renamed = mStore.getChild(mRoot, dir.getName());
    assertTrue(renamed.isPresent());
    assertTrue(mStore.getChild(renamed.get().asDirectory(), "dir" + (middleDir + 1)).isPresent());
    assertEquals(0,
        CloseableIterator.size(mStore.getChildren(mStore.get(middleDir - 1).get().asDirectory())));
  }
}
