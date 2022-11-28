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
import alluxio.client.WriteType;
import alluxio.collections.Pair;
import alluxio.concurrent.LockMode;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.CreatePathContext;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.DelegatingReadOnlyInodeStore;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.NewBlockEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
import alluxio.util.interfaces.Scoped;
import alluxio.wire.OperationId;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents the tree of Inodes.
 */
@NotThreadSafe
public interface InodeTreeInterface {
  /**
   * Patterns of inode path locking.
   */
  public enum LockPattern {
    /**
     * Read lock every existing inode and edge along the path. Useful when we want to read an inode
     * without modifying anything.
     *
     * Examples
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b
     * result: Read locks on [a, a->b, b]
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b/c
     * result: Read locks on [a, a->b, b, b->c, c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a
     * result: Read locks on [a]
     */
    READ,
    /**
     * Read lock every existing inode and edge along the path, but write lock the final inode if it
     * exists. If the inode does not exist, read lock the edge leading out of the final existing
     * ancestor. Useful when we want to modify an inode's metadata without changing the structure
     * of the inode tree (no create/rename/delete).
     *
     * Examples
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b
     * result: Read locks on [a, a->b, b, b->c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b/c
     * result: Read locks on [a, a->b, b, b->c], Write locks on [c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a
     * result: Read locks on [a, a->b]
     */
    WRITE_INODE,
    /**
     * Read lock every existing inode and edge along the path, but write lock the edge leading out
     * of the last existing ancestor. Useful when we want to modify the structure of the inode tree,
     * e.g. when creating, deleting, or renaming inodes.
     *
     * Examples
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b
     * result: Read locks on [a, a->b, b], Write locks on [b->c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a/b/c
     * result: Read locks on [a, a->b, b], Write locks on [b->c, c]
     *
     * path to lock: /a/b/c
     * existing inodes: /a
     * result: Read locks on [a], Write locks [a->b]
     */
    WRITE_EDGE;

    /**
     * @return whether the lock pattern is one of the write-type patterns
     */
    public boolean isWrite() {
      return this == WRITE_INODE || this == WRITE_EDGE;
    }
  }

  boolean isOperationComplete(@Nullable OperationId opId);

  void cacheOperation(@Nullable OperationId opId);

  void initializeRoot(String owner, String group, Mode mode, JournalContext context)
      throws UnavailableException;

  TtlBucketList getTtlBuckets();

  long getInodeCount();

  Map<Long, Number> getFileSizeHistogram();

  void setDirectChildrenLoaded(Supplier<JournalContext> context, InodeDirectory dir);

  long newBlock(Supplier<JournalContext> context, NewBlockEntry entry);

  void updateInodeFile(Supplier<JournalContext> context, UpdateInodeFileEntry entry);

  void updateInode(Supplier<JournalContext> context, UpdateInodeEntry entry);

  UpdateInodeEntry updateInodeAccessTimeNoJournal(long inodeId, long accessTime);

  void rename(Supplier<JournalContext> context, RenameEntry entry);

  void setAcl(Supplier<JournalContext> context, SetAclEntry entry);

  @Nullable
  String getRootUserName();

  int getPinnedSize();

  boolean inodeIdExists(long id);

  LockedInodePath tryLockInodePath(LockingScheme scheme, JournalContext journalContext)
      throws InvalidPathException;

  LockedInodePath lockInodePath(LockingScheme scheme, JournalContext journalContext)
      throws InvalidPathException;

  LockedInodePath lockInodePath(
      AlluxioURI uri, LockPattern lockPattern, JournalContext journalContext
  ) throws InvalidPathException;

  LockedInodePath lockInodePath(
      AlluxioURI uri, LockPattern lockPattern, boolean tryLock, JournalContext journalContext)
      throws InvalidPathException;

  boolean inodePathExists(AlluxioURI uri);

  LockedInodePath lockFullInodePath(
      AlluxioURI uri, LockingScheme lockScheme, JournalContext journalContext
  )
      throws InvalidPathException, FileDoesNotExistException;

  LockedInodePath lockFullInodePath(
      AlluxioURI uri, LockPattern lockPattern, JournalContext journalContext
  )
      throws InvalidPathException, FileDoesNotExistException;

  LockedInodePath lockFullInodePath(
      long id, LockPattern lockPattern, JournalContext journalContext
  )
      throws FileDoesNotExistException;

  InodePathPair lockInodePathPair(
      AlluxioURI path1, LockPattern lockPattern1,
      AlluxioURI path2, LockPattern lockPattern2,
      JournalContext journalContext
  ) throws InvalidPathException;

  void ensureFullInodePath(LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException;

  ArrayList<String> getPathInodeNames(InodeView inode) throws FileDoesNotExistException;

  ArrayList<String> getPathInodeNames(long id) throws FileDoesNotExistException;

  AlluxioURI getPath(long id) throws FileDoesNotExistException;

  AlluxioURI getPath(InodeView inode) throws FileDoesNotExistException;

  InodeDirectory getRoot();

  List<Inode> createPath(RpcContext rpcContext, LockedInodePath inodePath,
      CreatePathContext<?, ?> context) throws FileAlreadyExistsException, BlockInfoException,
      InvalidPathException, IOException, FileDoesNotExistException;

  LockedInodePathList getDescendants(LockedInodePath inodePath);

  void deleteInode(RpcContext rpcContext, LockedInodePath inodePath, long opTimeMs)
      throws FileDoesNotExistException;

  void setPinned(RpcContext rpcContext, LockedInodePath inodePath, boolean pinned,
      List<String> mediumTypes, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException;

  void setReplication(RpcContext rpcContext, LockedInodePath inodePath,
      Integer replicationMax, Integer replicationMin, long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException;

  Set<Long> getReplicationLimitedFileIds();

  Set<Long> getToBePersistedIds();

  Set<Long> getPinIdSet();

  InodeLockManager getInodeLockManager();

  boolean isRootId(long fileId);

  void syncPersistExistingDirectory(Supplier<JournalContext> context, InodeDirectoryView dir,
        boolean isMetadataLoad)
      throws IOException, InvalidPathException, FileDoesNotExistException;

  void syncPersistNewDirectory(
      MutableInodeDirectory dir, CreatePathContext<?, ?> createContext)
      throws InvalidPathException, FileDoesNotExistException, IOException;

  void close() throws IOException;
}
