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

import alluxio.AlluxioTestDirectory;
import alluxio.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.metastore.java.HeapInodeStore;
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

@RunWith(Parameterized.class)
public class InodeStoreTest {
  @Parameters
  public static Iterable<InodeStore> parameters() throws Exception {
    InstancedConfiguration conf = InstancedConfiguration.newBuilder()
        .setProperty(PropertyKey.MASTER_METASTORE_DIR,
            AlluxioTestDirectory.createTemporaryDirectory("inode-store-test"))
        .setProperty(PropertyKey.MASTER_METASTORE_INODE_CACHE_SIZE, 3)
        .build();
    return Arrays.asList(new HeapInodeStore(), new RocksInodeStore(conf),
        new CachingInodeStore(new RocksInodeStore(conf), conf));
  }

  private static final MutableInodeDirectory ROOT = inodeDir(0, -1, "");

  private final InodeStore mStore;

  public InodeStoreTest(InodeStore store) {
    mStore = store;
  }

  @Test
  public void get() {
    mStore.writeInode(ROOT);
    assertEquals(Inode.wrap(ROOT), mStore.get(0).get());
  }

  @Test
  public void getMutable() {
    mStore.writeInode(ROOT);
    assertEquals(ROOT, mStore.getMutable(0).get());
  }

  @Test
  public void getChild() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    mStore.writeInode(ROOT);
    mStore.writeInode(child);
    mStore.addChild(ROOT.getId(), child);
    assertEquals(Inode.wrap(child), mStore.getChild(ROOT, child.getName()).get());
  }

  @Test
  public void remove() {
    mStore.writeInode(ROOT);
    mStore.remove(ROOT);
  }

  @Test
  public void removeChild() {
    MutableInodeFile child = inodeFile(1, 0, "child");
    mStore.writeInode(ROOT);
    mStore.writeInode(child);
    mStore.addChild(ROOT.getId(), child);
    mStore.removeChild(ROOT.getId(), child.getName());
    assertFalse(mStore.getChild(ROOT, child.getName()).isPresent());
  }

  @Test
  public void updateInode() {
    mStore.writeInode(ROOT);
    MutableInodeDirectory mutableRoot = mStore.getMutable(ROOT.getId()).get().asDirectory();
    mutableRoot.setLastModificationTimeMs(163, true);
    mStore.writeInode(mutableRoot);
    assertEquals(163, mStore.get(ROOT.getId()).get().getLastModificationTimeMs());
  }

  @Test
  public void manyOperations() {
    mStore.writeInode(ROOT);
    MutableInodeDirectory curr = ROOT;
    List<Long> fileIds = new ArrayList<>();
    // Create 100 nested directories, each containing a file.
    for (int i = 1; i < 10; i++) {
      MutableInodeDirectory dir = inodeDir(i, curr.getId(), "dir" + i);
      MutableInodeFile file = inodeFile(i + 100, i, "file" + i);
      fileIds.add(file.getId());
      mStore.writeInode(dir);
      mStore.writeInode(file);
      mStore.addChild(curr.getId(), dir);
      mStore.addChild(dir.getId(), file);
      curr = dir;
    }

    // Check presence and delete files.
    for (int i = 0; i < 10; i++) {
      assertTrue(mStore.get(i).isPresent());
    }
    for (Long i : fileIds) {
      assertTrue(mStore.get(i).isPresent());
      mStore.remove(mStore.get(i).get());
      assertFalse(mStore.get(i).isPresent());
    }

    // Rename a directory
    MutableInodeDirectory dir = mStore.getMutable(5).get().asDirectory();
    mStore.removeChild(4, "dir5");
    mStore.addChild(ROOT.getId(), dir);
    dir.setParentId(ROOT.getId());
    mStore.writeInode(dir);

    Optional<Inode> renamed = mStore.getChild(ROOT, dir.getName());
    assertTrue(renamed.isPresent());
    assertTrue(mStore.getChild(renamed.get().asDirectory(), "dir6").isPresent());
    assertEquals(0, Iterables.size(mStore.getChildren(mStore.get(4).get().asDirectory())));
  }

  private static MutableInodeDirectory inodeDir(long id, long parentId, String name) {
    return MutableInodeDirectory.create(id, parentId, name, CreateDirectoryOptions.defaults());
  }

  private static MutableInodeFile inodeFile(long containerId, long parentId, String name) {
    return MutableInodeFile.create(containerId, parentId, name, 0, CreateFileOptions.defaults());
  }
}
