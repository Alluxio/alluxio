/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next.filesystem.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import tachyon.Constants;
import tachyon.master.next.IndexedSet;
import tachyon.thrift.FileInfo;

/**
 * Tachyon file system's folder representation in master.
 */
public final class InodeDirectory extends Inode {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private IndexedSet.FieldIndex<Inode> mIdIndex = new IndexedSet.FieldIndex<Inode>() {
    @Override
    public Object getFieldValue(Inode o) {
      return o.getId();
    }
  };
  private IndexedSet.FieldIndex<Inode> mNameIndex = new IndexedSet.FieldIndex<Inode>() {
    @Override
    public Object getFieldValue(Inode o) {
      return o.getName();
    }
  };
  @SuppressWarnings("unchecked")
  private IndexedSet<Inode> mChildren = new IndexedSet<Inode>(mIdIndex, mNameIndex);

  /**
   * Create a new InodeFolder.
   *
   * @param name The name of the folder
   * @param id The inode id of the folder
   * @param parentId The inode id of the parent of the folder
   * @param creationTimeMs The creation time of the folder, in milliseconds
   */
  public InodeDirectory(String name, long id, long parentId, long creationTimeMs) {
    super(name, id, parentId, true, creationTimeMs);
  }

  /**
   * Adds the given inode to the set of children.
   *
   * @param child The inode to add
   */
  public synchronized void addChild(Inode child) {
    mChildren.add(child);
  }

  /**
   * Adds the given inodes to the set of children.
   *
   * @param children The inodes to add
   */
  public synchronized void addChildren(Inode[] children) {
    for (Inode child : children) {
      addChild(child);
    }
  }

  /**
   * Generates client file info for the folder.
   *
   * @param path The path of the folder in the filesystem
   * @return the generated FileInfo
   */
  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();

    // TODO: make this a long.
    ret.fileId = (int) getId();
    ret.name = getName();
    ret.path = path;
    ret.ufsPath = "";
    ret.length = 0;
    ret.blockSizeByte = 0;
    ret.creationTimeMs = getCreationTimeMs();
    ret.isComplete = true;
    ret.isFolder = true;
    ret.isPinned = isPinned();
    ret.isCache = false;
    ret.blockIds = null;
    ret.dependencyId = -1;
    ret.lastModificationTimeMs = getLastModificationTimeMs();

    return ret;
  }

  /**
   * Returns the child with the given inode id.
   *
   * @param id The inode id of the child
   * @return the inode with the given id, or null if there is no child with that id
   */
  public synchronized Inode getChild(long id) {
    return mChildren.getFirstByField(mIdIndex, id);
  }

  /**
   * Returns the child with the given name.
   *
   * @param name The name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  public synchronized Inode getChild(String name) {
    return mChildren.getFirstByField(mNameIndex, name);
  }

  /**
   * Returns the folder's children.
   *
   * @return an unmodifiable set of the children inodes.
   */
  public synchronized Set<Inode> getChildren() {
    return ImmutableSet.copyOf(mChildren.all());
  }

  /**
   * Returns the ids of the children.
   *
   * @return the ids of the children
   */
  public synchronized List<Long> getChildrenIds() {
    Set<Inode> children = mChildren.all();
    List<Long> ret = new ArrayList<Long>(children.size());
    for (Inode inode : children) {
      ret.add(inode.getId());
    }
    return ret;
  }

  /**
   * Returns the number of children the folder has.
   *
   * @return the number of children in the folder.
   */
  public synchronized int getNumberOfChildren() {
    return mChildren.size();
  }

  /**
   * Removes the given inode from the folder.
   *
   * @param child The Inode to remove
   * @return true if the inode was removed, false otherwise.
   */
  public synchronized boolean removeChild(Inode child) {
    return mChildren.remove(child);
  }

  /**
   * Removes the given child by its name from the folder.
   *
   * @param name The name of the Inode to remove.
   * @return true if the inode was removed, false otherwise.
   */
  public synchronized boolean removeChild(String name) {
    return mChildren.removeByField(mNameIndex, name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFolder(");
    sb.append(super.toString()).append(",").append(mChildren.all()).append(")");
    return sb.toString();
  }
}
