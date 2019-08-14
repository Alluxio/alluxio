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

import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * Wrapper for providing read-only access to an inode store.
 */
public class DelegatingReadOnlyInodeStore implements ReadOnlyInodeStore {
  private final InodeStore mDelegate;

  /**
   * @param delegate the delegate inode store
   */
  public DelegatingReadOnlyInodeStore(InodeStore delegate) {
    mDelegate = delegate;
  }

  @Override
  public Optional<Inode> get(long id, ReadOption option) {
    return mDelegate.get(id, option);
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId, ReadOption option) {
    return mDelegate.getChildIds(inodeId, option);
  }

  @Override
  public Iterable<Long> getChildIds(InodeDirectoryView inode, ReadOption option) {
    return mDelegate.getChildIds(inode, option);
  }

  @Override
  public Iterable<? extends Inode> getChildren(Long inodeId, ReadOption option) {
    return mDelegate.getChildren(inodeId, option);
  }

  @Override
  public Iterable<? extends Inode> getChildren(InodeDirectoryView inode, ReadOption option) {
    return mDelegate.getChildren(inode, option);
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    return mDelegate.getChildId(inodeId, name, option);
  }

  @Override
  public Optional<Long> getChildId(InodeDirectoryView inode, String name, ReadOption option) {
    return mDelegate.getChildId(inode, name, option);
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
    return mDelegate.getChild(inodeId, name, option);
  }

  @Override
  public Optional<Inode> getChild(InodeDirectoryView inode, String name, ReadOption option) {
    return mDelegate.getChild(inode, name, option);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode, ReadOption option) {
    return mDelegate.hasChildren(inode, option);
  }

  @Override
  @VisibleForTesting
  public Set<EdgeEntry> allEdges() {
    return mDelegate.allEdges();
  }

  @Override
  @VisibleForTesting
  public Set<MutableInode<?>> allInodes() {
    return mDelegate.allInodes();
  }

  @Override
  public void close() throws IOException {
    mDelegate.close();
  }
}
