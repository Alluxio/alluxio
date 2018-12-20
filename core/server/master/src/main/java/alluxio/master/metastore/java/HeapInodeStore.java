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

package alluxio.master.metastore.java;

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;
import alluxio.master.metastore.InodeStore;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FileStore implementation using on-heap data structures.
 */
public class HeapInodeStore implements InodeStore {
  private final Map<Long, Inode<?>> mInodes = new ConcurrentHashMap<>();
  private final Map<Long, TreeMap<String, Inode<?>>> mEdges = new ConcurrentHashMap<>();

  @Override
  public void remove(InodeView inode) {
    mInodes.remove(inode.getId());
    removeChild(inode.getParentId(), inode.getName());
  }

  @Override
  public void setModificationTimeMs(long id, long opTimeMs) {
    mInodes.get(id).setLastModificationTimeMs(opTimeMs);
  }

  @Override
  public void writeInode(Inode<?> inode) {
    mInodes.putIfAbsent(inode.getId(), inode);
  }

  @Override
  public void addChild(long parentId, InodeView child) {
    mEdges.putIfAbsent(parentId, new TreeMap<>());
    mEdges.get(parentId).put(child.getName(), (Inode<?>) child);
  }

  @Override
  public void removeChild(long parentId, String name) {
    if (mEdges.containsKey(parentId)) {
      mEdges.get(parentId).remove(name);
    }
  }

  @Override
  public int size() {
    return mInodes.size();
  }

  @Override
  public Optional<InodeView> get(long id) {
    return Optional.ofNullable(mInodes.get(id));
  }

  @Override
  public boolean hasChild(long parentId, String name) {
    return mEdges.containsKey(parentId) && mEdges.get(parentId).containsKey(name);
  }

  @Override
  public Iterable<? extends InodeView> getChildren(long id) {
    if (!mEdges.containsKey(id)) {
      return Collections.emptySet();
    }

    return mEdges.get(id).values();
  }

  @Override
  public Optional<InodeView> getChild(long parentId, String child) {
    if (!mEdges.containsKey(parentId)) {
      return Optional.empty();
    }
    return Optional.ofNullable(mEdges.get(parentId).get(child));
  }

  @Override
  public boolean hasChildren(long id) {
    return mEdges.containsKey(id) && !mEdges.get(id).isEmpty();
  }

  @Override
  public void clear() {
    mInodes.clear();
    mEdges.clear();
  }
}
