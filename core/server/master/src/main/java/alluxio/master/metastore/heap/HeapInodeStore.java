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

import alluxio.collections.TwoKeyConcurrentSortedMap;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
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
import alluxio.resource.CloseableIterator;
import alluxio.util.ObjectSizeCalculator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.StreamSupport;
import javax.annotation.concurrent.ThreadSafe;

/**
 * FileStore implementation using on-heap data structures.
 */
@ThreadSafe
public class HeapInodeStore implements InodeStore {
  public final Map<Long, MutableInode<?>> mInodes = new ConcurrentHashMap<>();
  // Map from inode id to ids of children of that inode. The inner maps are ordered by child name.
  private final TwoKeyConcurrentSortedMap<Long, String, Long, SortedMap<String, Long>> mEdges =
      new TwoKeyConcurrentSortedMap<>(ConcurrentSkipListMap::new);

  /**
   * Construct a heap inode store.
   */
  public HeapInodeStore() {
    super();
    if (Configuration.getBoolean(PropertyKey.MASTER_METRICS_HEAP_ENABLED)) {
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
  public CloseableIterator<? extends Inode> getChildren(Long inodeId, ReadOption option) {
    CloseableIterator<Long> childIter = getChildIds(inodeId, option);
    return CloseableIterator.create(StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(childIter, Spliterator.ORDERED), false)
        .map(this::get)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(Inode::wrap).iterator(), (any) -> childIter.close());
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
    return mEdges.flattenEntries(EdgeEntry::new);
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

  private SortedMap<String, Long> children(long id) {
    return mEdges.getOrDefault(id, Collections.emptySortedMap());
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

  @Override
  public CloseableIterator<Long> getChildIds(Long inodeId, ReadOption option) {
    return CloseableIterator.noopCloseable(sortedMapToIterator(children(inodeId), option));
  }

  /**
   * Helper function that returns an iterator over the sorted map using the given
   * read options.
   * @param childrenMap the map to create the iterator from
   * @param option the read options to use
   * @return the iterator over the map
   */
  public static Iterator<Long> sortedMapToIterator(
      SortedMap<String, Long> childrenMap, ReadOption option) {

    if (option.getStartFrom() != null && option.getPrefix() != null) {
      // if the prefix is after readFrom, then we just start the map from
      // the prefix
      if (option.getPrefix().compareTo(option.getStartFrom()) > 0) {
        childrenMap = childrenMap.tailMap(option.getPrefix());
      } else {
        childrenMap = childrenMap.tailMap(option.getStartFrom());
      }
    } else if (option.getStartFrom() != null) {
      childrenMap = childrenMap.tailMap(option.getStartFrom());
    } else if (option.getPrefix() != null) {
      childrenMap = childrenMap.tailMap(option.getPrefix());
    }

    if (option.getPrefix() == null) {
      return childrenMap.values().iterator();
    } else {
      // make an iterator that stops once the prefix has been passed
      class PrefixIter implements Iterator<Long> {
        final Iterator<Map.Entry<String, Long>> mIter;
        Map.Entry<String, Long> mNxt;

        PrefixIter(Iterator<Map.Entry<String, Long>> iter) {
          mIter = iter;
          checkNext();
        }

        @Override
        public boolean hasNext() {
          return (mNxt != null);
        }

        @Override
        public Long next() {
          Long val = mNxt.getValue();
          checkNext();
          return val;
        }

        private void checkNext() {
          mNxt = null;
          if (mIter.hasNext()) {
            mNxt = mIter.next();
            if (!mNxt.getKey().startsWith(option.getPrefix())) {
              mNxt = null;
            }
          }
        }
      }

      return new PrefixIter(childrenMap.entrySet().iterator());
    }
  }
}
