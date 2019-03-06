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

package alluxio.master.metastore.heap;

import static java.util.stream.Collectors.toList;

import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.metastore.InodeStore;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * FileStore implementation using on-heap data structures.
 */
@ThreadSafe
public class HeapInodeStore implements InodeStore {
  private final Map<Long, MutableInode<?>> mInodes = new ConcurrentHashMap<>();
  // Map from inode id to ids of children of that inode. The inner maps are ordered by child name.
  private final TwoKeyConcurrentMap<Long, String, Long, Map<String, Long>> mEdges =
      new TwoKeyConcurrentMap<>(() -> new ConcurrentHashMap<>(4));

  /**
   * @param args inode store arguments
   */
  public HeapInodeStore(InodeStoreArgs args) {}

  @Override
  public void remove(Long inodeId) {
    mInodes.remove(inodeId);
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mInodes.putIfAbsent(inode.getId(), inode);
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    mEdges.addInnerValue(parentId, childName, childId);
  }

  @Override
  public void removeChild(long parentId, String name) {
    mEdges.removeInnerValue(parentId, name);
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id) {
    return Optional.ofNullable(mInodes.get(id));
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId) {
    return children(inodeId).values();
  }

  @Override
  public Iterable<? extends Inode> getChildren(Long inodeId) {
    return children(inodeId).values().stream()
        .map(this::get)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(Inode::wrap)
        .collect(toList());
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String child) {
    return Optional.ofNullable(children(inodeId).get(child));
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String child) {
    return getChildId(inodeId, child)
        .flatMap(this::get)
        .map(Inode::wrap);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView dir) {
    return !children(dir.getId()).isEmpty();
  }

  @Override
  public Set<EdgeEntry> allEdges() {
    return mEdges.flattenEntries((a, b, c) -> new EdgeEntry(a, b, c));
  }

  @Override
  public Set<MutableInode<?>> allInodes() {
    return new HashSet<>(mInodes.values());
  }

  @Override
  public void clear() {
    mInodes.clear();
    mEdges.clear();
  }

  private Map<String, Long> children(long id) {
    return mEdges.getOrDefault(id, Collections.emptyMap());
  }
}
