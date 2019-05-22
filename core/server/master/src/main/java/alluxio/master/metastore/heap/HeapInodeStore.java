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

import alluxio.Constants;
import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat;
import alluxio.master.journal.checkpoint.LongsCheckpointFormat;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.proto.meta.InodeMeta;

import com.esotericsoftware.kryo.io.OutputChunked;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
  /** Map for storing indices. */
  private HashMap<InodeIndice, Set<Long>> mIndices;

  /**
   * Creates a default instance.
   */
  public HeapInodeStore() {
    mIndices = new HashMap<>();
    for (InodeIndice indice : InodeIndice.values()) {
      mIndices.put(indice, ConcurrentHashMap.newKeySet());
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
  public void setIndice(long id, InodeIndice indice, boolean isSet) {
    if (isSet) {
      mIndices.get(indice).add(id);
    } else {
      mIndices.get(indice).remove(id);
    }
  }

  @Override
  public void clearIndices(InodeIndice indice) {
    mIndices.get(indice).clear();
  }

  @Override
  public Iterator<Long> getIndiced(InodeIndice indice) {
    return Collections.unmodifiableSet(mIndices.get(indice)).iterator();
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
    for (InodeIndice indice : InodeIndice.values()) {
      mIndices.get(indice).clear();
    }
  }

  private Map<String, Long> children(long id) {
    return mEdges.getOrDefault(id, Collections.emptyMap());
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    OutputChunked chunked = new OutputChunked(
        new CheckpointOutputStream(output, CheckpointType.COMPOUND), 64 * Constants.KB);
    // Write inodes
    chunked.writeString(CheckpointName.HEAP_INODE_STORE_INODES.toString());
    OutputStream inodesOutput = new CheckpointOutputStream(chunked, CheckpointType.INODE_PROTOS);
    for (MutableInode<?> inode : mInodes.values()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      inode.toProto().writeDelimitedTo(inodesOutput);
    }
    chunked.endChunks();
    // Write indices
    for (InodeIndice indice : InodeIndice.values()) {
      String indiceCheckpointName = "HEAP_INODE_STORE_INDICES_" + indice.name();
      chunked.writeString(indiceCheckpointName);
      CheckpointOutputStream indiceOutput =
          new CheckpointOutputStream(chunked, CheckpointType.LONGS);
      for (Long id : mIndices.get(indice)) {
        indiceOutput.writeLong(id);
      }
      chunked.endChunks();
    }
    chunked.flush();
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    CompoundCheckpointFormat.CompoundCheckpointReader reader =
        new CompoundCheckpointFormat.CompoundCheckpointReader(input);
    Optional<CompoundCheckpointFormat.CompoundCheckpointReader.Entry> next;
    while ((next = reader.nextCheckpoint()).isPresent()) {
      CompoundCheckpointFormat.CompoundCheckpointReader.Entry nextEntry = next.get();
      if (nextEntry.getName() == CheckpointName.HEAP_INODE_STORE_INODES) {
        CheckpointInputStream inodeInput = nextEntry.getStream();
        Preconditions.checkState(inodeInput.getType() == CheckpointType.INODE_PROTOS,
            "Unexpected checkpoint type in heap inode store: " + input.getType());
        InodeMeta.Inode inodeProto;
        while ((inodeProto = InodeMeta.Inode.parseDelimitedFrom(inodeInput)) != null) {
          MutableInode<?> inode = MutableInode.fromProto(inodeProto);
          mInodes.put(inode.getId(), inode);
          mEdges.addInnerValue(inode.getParentId(), inode.getName(), inode.getId());
        }
      } else {
        String indiceCheckpointNameHeader = "HEAP_INODE_STORE_INDICES_";
        String indiceCheckpointName = nextEntry.getName().toString();
        InodeIndice indice = null;
        if (indiceCheckpointName.startsWith(indiceCheckpointNameHeader)) {
          indice = InodeIndice
              .valueOf(indiceCheckpointName.substring(indiceCheckpointNameHeader.length()));
        }
        if (indice == null) {
          throw new RuntimeException(
              String.format("Unrecognized checkpoint name: %s", nextEntry.getName()));
        }
        LongsCheckpointFormat.LongsCheckpointReader indiceReader =
            new LongsCheckpointFormat.LongsCheckpointReader(nextEntry.getStream());
        Optional<Long> longOpt;
        while ((longOpt = indiceReader.nextLong()).isPresent()) {
          mIndices.get(indice).add(longOpt.get());
        }
      }
    }
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.HEAP_INODE_STORE;
  }
}
