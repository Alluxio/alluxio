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

import alluxio.grpc.TtlAction;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Base class for read only inodes.
 */
public abstract class Inode implements InodeView {
  private final InodeView mDelegate;

  protected Inode(InodeView delegate) {
    mDelegate = delegate;
  }

  @Override
  public long getCreationTimeMs() {
    return mDelegate.getCreationTimeMs();
  }

  @Override
  public String getGroup() {
    return mDelegate.getGroup();
  }

  @Override
  public long getId() {
    return mDelegate.getId();
  }

  @Override
  public long getTtl() {
    return mDelegate.getTtl();
  }

  @Override
  public TtlAction getTtlAction() {
    return mDelegate.getTtlAction();
  }

  @Override
  public long getLastModificationTimeMs() {
    return mDelegate.getLastModificationTimeMs();
  }

  @Override
  public long getLastAccessTimeMs() {
    return mDelegate.getLastAccessTimeMs();
  }

  @Override
  public String getName() {
    return mDelegate.getName();
  }

  @Override
  public short getMode() {
    return mDelegate.getMode();
  }

  @Override
  public PersistenceState getPersistenceState() {
    return mDelegate.getPersistenceState();
  }

  @Override
  public long getParentId() {
    return mDelegate.getParentId();
  }

  @Override
  public String getOwner() {
    return mDelegate.getOwner();
  }

  @Override
  @Nullable
  public Map<String, byte[]> getXAttr() {
    return mDelegate.getXAttr();
  }

  @Override
  public boolean isDeleted() {
    return mDelegate.isDeleted();
  }

  @Override
  public boolean isDirectory() {
    return mDelegate.isDirectory();
  }

  @Override
  public boolean isFile() {
    return mDelegate.isFile();
  }

  @Override
  public boolean isPinned() {
    return mDelegate.isPinned();
  }

  @Override
  public boolean isPersisted() {
    return mDelegate.isPersisted();
  }

  @Override
  public String getUfsFingerprint() {
    return mDelegate.getUfsFingerprint();
  }

  @Override
  public AccessControlList getACL() {
    return mDelegate.getACL();
  }

  @Override
  public DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException {
    return mDelegate.getDefaultACL();
  }

  @Override
  public Set<String> getMediumTypes() {
    return mDelegate.getMediumTypes();
  }

  @Override
  public FileInfo generateClientFileInfo(String path) {
    return mDelegate.generateClientFileInfo(path);
  }

  @Override
  public boolean checkPermission(String user, List<String> groups, AclAction action) {
    return mDelegate.checkPermission(user, groups, action);
  }

  @Override
  public AclActions getPermission(String user, List<String> groups) {
    return mDelegate.getPermission(user, groups);
  }

  @Override
  public InodeMeta.Inode toProto() {
    return mDelegate.toProto();
  }

  @Override
  public JournalEntry toJournalEntry() {
    return mDelegate.toJournalEntry();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Inode)) {
      return false;
    }
    Inode otherInode = (Inode) other;
    return mDelegate.equals(otherInode.mDelegate);
  }

  @Override
  public int hashCode() {
    return mDelegate.hashCode();
  }

  /**
   * Casts the inode as an {@link InodeDirectory} if it is one, otherwise throws an
   * exception.
   *
   * This gives convenience in method chaining, e.g.
   *
   * inode.asDirectory().getChildren()
   *
   * instead of
   *
   * ((InodeDirectory) inode).getChildren()
   *
   * @return the inode as an inode directory
   */
  public InodeDirectory asDirectory() {
    if (!isDirectory()) {
      throw new IllegalStateException(
          String.format("Inode %s is not a directory", mDelegate.getName()));
    }
    return (InodeDirectory) this;
  }

  /**
   * @return the inode as an inode file
   */
  public InodeFile asFile() {
    if (isDirectory()) {
      throw new IllegalStateException(
          String.format("Inode %s is not a file", mDelegate.getName()));
    }
    return (InodeFile) this;
  }

  /**
   * Wraps an InodeView, providing read-only access. Modifications to the underlying inode will
   * affect the created read-only inode.
   *
   * @param delegate the delegate to wrap
   * @return the created read-only inode
   */
  public static Inode wrap(InodeView delegate) {
    if (delegate instanceof Inode) {
      return (Inode) delegate;
    }
    if (delegate.isFile()) {
      Preconditions.checkState(delegate instanceof InodeFileView);
      return new InodeFile((InodeFileView) delegate);
    } else {
      Preconditions.checkState(delegate instanceof InodeDirectoryView);
      return new InodeDirectory((InodeDirectoryView) delegate);
    }
  }
}
