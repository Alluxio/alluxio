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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.InodeMeta;
import alluxio.util.ObjectSizeCalculator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.OutputStream;
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
   * Construct a heap inode store.
   */
  public HeapInodeStore() {
    super();
    if (ServerConfiguration.getBoolean(PropertyKey.MASTER_METRICS_HEAP_ENABLED)) {
      MetricsSystem.registerCachedGaugeIfAbsent(MetricKey.MASTER_INODE_HEAP_SIZE.getName(),
          () -> ObjectSizeCalculator.getObjectSize(mInodes,
          ImmutableSet.of(Long.class, MutableInodeFile.class, MutableInodeDirectory.class)));
    }
  }

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
  public Optional<MutableInode<?>> getMutable(long id, ReadOption option) {
    return Optional.ofNullable(mInodes.get(id));
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId, ReadOption option) {
    return children(inodeId).values();
  }

  @Override
  public Iterable<? extends Inode> getChildren(Long inodeId, ReadOption option) {
    return children(inodeId).values().stream()
        .map(this::get)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(Inode::wrap)
        .collect(toList());
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String child, ReadOption option) {
    return Optional.ofNullable(children(inodeId).get(child));
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String child, ReadOption option) {
    return getChildId(inodeId, child)
        .flatMap(this::get)
        .map(Inode::wrap);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView dir, ReadOption option) {
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

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    output = new CheckpointOutputStream(output, CheckpointType.INODE_PROTOS);
    for (MutableInode<?> inode : mInodes.values()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      inode.toProto().writeDelimitedTo(output);
    }
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    Preconditions.checkState(input.getType() == CheckpointType.INODE_PROTOS,
        "Unexpected checkpoint type in heap inode store: " + input.getType());
    InodeMeta.Inode inodeProto;
    while ((inodeProto = InodeMeta.Inode.parseDelimitedFrom(input)) != null) {
      MutableInode<?> inode = MutableInode.fromProto(inodeProto);
      mInodes.put(inode.getId(), inode);
      mEdges.addInnerValue(inode.getParentId(), inode.getName(), inode.getId());
    }
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.HEAP_INODE_STORE;
  }
}
