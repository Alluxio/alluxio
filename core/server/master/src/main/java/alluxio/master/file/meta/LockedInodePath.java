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

import alluxio.AlluxioURI;
import alluxio.concurrent.LockMode;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.meta.InodeTreeInterface.LockPattern;
import alluxio.master.journal.JournalContext;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.resource.AlluxioResourceLeakDetectorFactory;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents a locked path within the inode tree, starting from the root.
 *
 * The mUri and mPathComponents fields are immutable and contain the "target" path represented by
 * the LockedInodePath.
 *
 * mLockList manages the locks held by the LockedInodePath.
 *
 * The mExistingInodes list holds inodes that are locked and known to exist. The inodes might not be
 * individually locked - if one inode is write-locked, the inodes after it are implicitly
 * write-locked and may be added to mExistingInodes. The inodes in mExistingInodes are a prefix of
 * the inodes referenced by mPathComponents.
 *
 * If the full path exists, mExistingInodes will have size equal to mPathComponents.length after
 * traversal. mExistingInodes is always at least as long as the list of inodes directly locked by
 * mLockList.
 *
 * To create new inode paths from an existing path, use one of the lock*() methods. They return new
 * locked inode paths that can be modified and closed without affecting the original path. Modifying
 * the original inode path invalidates any subpaths that it has created.
 *
 * Locked inode paths are not threadsafe and should not be shared across threads.
 */
@NotThreadSafe
public interface LockedInodePath extends Closeable {
  /**
   * @return the full uri of the path
   */
  public AlluxioURI getUri();

  /**
   * @return the target inode
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public Inode getInode() throws FileDoesNotExistException;

  @Nullable
  public Inode getInodeOrNull();

  public InodeFile getInodeFile() throws FileDoesNotExistException;

  public InodeDirectory getParentInodeDirectory()
      throws InvalidPathException, FileDoesNotExistException;

  @Nullable
  public Inode getParentInodeOrNull();

  public Inode getLastExistingInode();

  public List<Inode> getInodeList();

  public List<InodeView> getInodeViewList();

  public int getExistingInodeCount();

  public int size();

  public boolean fullPathExists();

  public LockPattern getLockPattern();

  public void removeLastInode();

  public void addNextInode(Inode inode);

  public void downgradeToRead();

  public Inode getAncestorInode() throws FileDoesNotExistException;

  public LockedInodePath lockDescendant(AlluxioURI descendantUri, LockPattern lockPattern)
      throws InvalidPathException;

  public LockedInodePath lockChild(Inode child, LockPattern lockPattern)
      throws InvalidPathException;

  public LockedInodePath lockChild(Inode child, LockPattern lockPattern,
      String[] childComponentsHint) throws InvalidPathException;

  public LockedInodePath lockChildByName(String childName, LockPattern lockPattern,
      String[] childComponentsHint) throws InvalidPathException;

  public LockedInodePath lockFinalEdgeWrite() throws InvalidPathException;

  public void traverse() throws InvalidPathException;

  @Override
  public void close();
}
