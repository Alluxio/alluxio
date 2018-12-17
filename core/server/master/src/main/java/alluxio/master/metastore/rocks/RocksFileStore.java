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

package alluxio.master.metastore.rocks;

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;
import alluxio.master.metastore.InodeStore;

import java.util.Optional;

/**
 * File store backed by RocksDB.
 */
public class RocksFileStore implements InodeStore {
  @Override
  public void remove(InodeView inode) {
  }

  @Override
  public void setModificationTimeMs(long id, long opTimeMs) {
  }

  @Override
  public void writeInode(Inode<?> inode) {
  }

  @Override
  public void clear() {
  }

  @Override
  public void addChild(long parentId, InodeView inode) {
  }

  @Override
  public void removeChild(long parentId, String name) {
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Optional<InodeView> get(long id) {
    return Optional.empty();
  }

  @Override
  public boolean hasChild(long parentId, String name) {
    return false;
  }

  @Override
  public Iterable<? extends InodeView> getChildren(long id) {
    return null;
  }

  @Override
  public Optional<InodeView> getChild(long id, String pathComponent) {
    return Optional.empty();
  }

  @Override
  public boolean hasChildren(long id) {
    return false;
  }
}
