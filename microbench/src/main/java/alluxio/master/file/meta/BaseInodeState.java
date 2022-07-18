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

package alluxio.master.file.meta;

import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseInodeState {
  protected InodeLockManager mInodeLockManager = new InodeLockManager();
  protected InodeStore mInodeStore = new HeapInodeStore();

  protected InodeDirectory mDirMnt = inodeDir(1, 0, "mnt");
  protected InodeDirectory mRootDir = inodeDir(0, -1, "", mDirMnt);
  protected List<Inode> mInodes = new ArrayList<>(Arrays.asList(mRootDir, mDirMnt));

  protected InodeDirectory createInodeDir(InodeDirectory parentDir, String name) {
    InodeDirectory dir = inodeDir(mInodes.size(), parentDir.getId(), name);
    mInodes.add(dir);
    mInodeStore.addChild(parentDir.getId(), dir);
    return dir;
  }

  protected InodeFile createInodeFile(InodeDirectory parentDir, String name) {
    InodeFile dir = inodeFile(mInodes.size(), parentDir.getId(), name);
    mInodes.add(dir);
    mInodeStore.addChild(parentDir.getId(), dir);
    return dir;
  }

  private InodeDirectory inodeDir(long id, long parentId, String name, Inode... children) {
    MutableInodeDirectory dir =
        MutableInodeDirectory.create(id, parentId, name, CreateDirectoryContext.defaults());
    mInodeStore.writeInode(dir);
    for (Inode child : children) {
      mInodeStore.addChild(dir.getId(), child);
    }
    return Inode.wrap(dir).asDirectory();
  }

  private InodeFile inodeFile(long id, long parentId, String name) {
    MutableInodeFile file =
        MutableInodeFile.create(id, parentId, name, 0, CreateFileContext.defaults());
    mInodeStore.writeInode(file);
    return Inode.wrap(file).asFile();
  }
}
