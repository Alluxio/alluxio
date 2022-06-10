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
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.File.DeleteMountPointEntry;
import alluxio.proto.journal.File.StringPairEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import com.codahale.metrics.Counter;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is used for keeping track of Alluxio mount points.
 */
@ThreadSafe
public final class MountTable implements DelegatingJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(MountTable.class);

  public static final String ROOT = "/";

  private final Lock mReadLock;
  private final Lock mWriteLock;

  /** Mount table state that is preserved across restarts. */
  @GuardedBy("mReadLock,mWriteLock")
  public final State mState;

  /** The manager of all ufs. */
  private final UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link MountTable}.
   *
   * @param ufsManager the UFS manager
   * @param rootMountInfo root mount info
   * @param clock the clock
   */
  public MountTable(UfsManager ufsManager, MountInfo rootMountInfo, Clock clock) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    mReadLock = lock.readLock();
    mWriteLock = lock.writeLock();
    mUfsManager = ufsManager;
    mState = new State(rootMountInfo, clock);
  }

  /**
   * Returns the underlying writelock of the MountTable. This method will be called when
   * fileSystemMaster is adding a new MountPoint.
   *
   * @return the write lock of the mountTable
   */
  public Lock getWriteLock() {
    return mWriteLock;
  }

  /**
   * Mounts the given UFS path at the given Alluxio path. The Alluxio path should not be nested
   * under an existing mount point. It will first check the state of MountTableTrie and the
   * lockedPath, and call
   * {@link MountTable#add(Supplier, List, AlluxioURI, AlluxioURI, long, MountPOptions)}.
   *
   * @param journalContext the journal context
   * @param alluxioLockedPath an Alluxio Locked Path
   * @param ufsUri a UFS path URI
   * @param mountId the mount id
   * @param options the mount options
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public void add(Supplier<JournalContext> journalContext, AlluxioURI alluxioUri, AlluxioURI ufsUri,
      long mountId, MountPOptions options) throws FileAlreadyExistsException, InvalidPathException,
      IOException {
    try (LockResource r = new LockResource(mWriteLock)) {
      // validate the Mount operation first, error will be thrown if the operation is invalid
      validateMountPoint(alluxioUri, ufsUri, mountId, options);
      addValidated(journalContext, alluxioUri, ufsUri, mountId, options);
    }
  }

  /**
   * Inserts an entry into the mount table.
   * Before calling this method, the caller must hold the write lock to the mount table.
   * The caller must also have validated the mount information to make sure the mount will succeed.
   * <blockquote><pre>
   * try (LockResource mountTableLock = new LockResource(mMountTable.getWriteLock()) {
   *     mMountTable.validateMountPoint(alluxioPath, ufsPath);
   *     mMountTable.addValidated(alluxioPath, ufsPath);
   *     ...
   *  }
   * </pre></blockquote>
   * @param journalContext the journal context
   * @param alluxioUri the uri of Alluxio Mount Point
   * @param ufsUri the uri of UFS Path
   * @param mountId the mount id
   * @param options the mount options
   */
  public void addValidated(Supplier<JournalContext> journalContext,
      AlluxioURI alluxioUri, AlluxioURI ufsUri, long mountId, MountPOptions options) {
    String alluxioPath = alluxioUri.getPath().isEmpty() ? "/" : alluxioUri.getPath();
    LOG.info("Mounting {} at {}", ufsUri, alluxioPath);

    mState.applyAndJournal(journalContext,
        createMountPointInfo(alluxioPath, ufsUri, mountId, options));
  }

  /**
   * Verify if the given (alluxioPath, ufsPath) can be inserted into MountTable. This method is
   * NOT ThreadSafe. This method will not acquire any locks, so the caller MUST apply the lock
   * first before calling this method.
   * @param alluxioUri the alluxio path that is about to be the mountpoint
   * @param ufsUri the UFS path that is about to mount
   * @param mountId the mount id
   * @param options the mount options
   */
  public void validateMountPoint(AlluxioURI alluxioUri, AlluxioURI ufsUri, long mountId,
      MountPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    String alluxioPath = alluxioUri.getPath().isEmpty() ? "/" : alluxioUri.getPath();
    LOG.info("Validating Mounting {} at {}, with id {} and options {}",
        ufsUri, alluxioPath, mountId, options);
    if (mState.getMountTable().containsKey(alluxioPath)) {
      throw new FileAlreadyExistsException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(alluxioPath));
    }
    // Make sure that the ufs path we're trying to mount is not a prefix
    // or suffix of any existing mount path.
    for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
      AlluxioURI mountedUfsUri = entry.getValue().getUfsUri();
      if ((ufsUri.getScheme() == null || ufsUri.getScheme().equals(mountedUfsUri.getScheme()))
          && (ufsUri.getAuthority().toString().equals(mountedUfsUri.getAuthority().toString()))) {
        String ufsPath = ufsUri.getPath().isEmpty() ? "/" : ufsUri.getPath();
        String mountedUfsPath = mountedUfsUri.getPath().isEmpty() ? "/" : mountedUfsUri.getPath();
        if (PathUtils.hasPrefix(ufsPath, mountedUfsPath)) {
          throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER
              .getMessage(mountedUfsUri.toString(), ufsUri.toString()));
        }
        if (PathUtils.hasPrefix(mountedUfsPath, ufsPath)) {
          throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER
              .getMessage(ufsUri.toString(), mountedUfsUri.toString()));
        }
      }
    }

    // Check that the alluxioPath we're creating doesn't shadow a path in the parent UFS
    MountTable.Resolution resolution = resolve(alluxioUri);
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      String ufsResolvedPath = resolution.getUri().getPath();
      if (ufsResource.get().exists(ufsResolvedPath)) {
        throw new InvalidPathException(MessageFormat.format(
            "Mount path {0} shadows an existing path {1} in the parent underlying filesystem",
            alluxioPath, ufsResolvedPath));
      }
    }
  }

  /**
   * Helper function to generate AddMountPointEntry.
   */
  private static AddMountPointEntry createMountPointInfo(
      String alluxioPath, AlluxioURI ufsUri, long mountId, MountPOptions options) {
    Map<String, String> properties = options.getPropertiesMap();
    return AddMountPointEntry.newBuilder()
        .addAllProperties(properties.entrySet().stream()
            .map(entry -> StringPairEntry.newBuilder()
                .setKey(entry.getKey()).setValue(entry.getValue()).build())
            .collect(Collectors.toList()))
        .setAlluxioPath(alluxioPath)
        .setMountId(mountId)
        .setReadOnly(options.getReadOnly())
        .setShared(options.getShared())
        .setUfsPath(ufsUri.toString())
        .build();
  }

  /**
   * @param mountId the mount id
   * @return the ufs sync counter metric for the mount id
   */
  public Counter getUfsSyncMetric(long mountId) {
    return MetricsSystem.counter(MetricKey.getSyncMetricName(mountId));
  }

  /**
   * Unmounts the given Alluxio path. The path should match an existing mount point. It will
   * first check if MountTableTrie is enabled and the alluxioLockedPath is fullPathExists, then
   * decide how to call {@link MountTable#delete(Supplier, List, AlluxioURI, boolean)}.
   *
   * @param journalContext journal context
   * @param alluxioLockedPath an Alluxio Locked Path
   * @param checkNestedMount whether to check nested mount points before delete
   * @return whether the operation succeeded or not
   */
  public boolean delete(Supplier<JournalContext> journalContext, LockedInodePath alluxioLockedPath,
      boolean checkNestedMount) {
    if (mState.getMountTableTrie().isEnabled()) {
      return delete(journalContext, alluxioLockedPath.getInodeViewListWithEmptyInodes(),
          alluxioLockedPath.getUri(), checkNestedMount);
    }
    // an empty list tells delete that there is no need to update the MountTableTrie.
    return delete(journalContext, Collections.emptyList(), alluxioLockedPath.getUri(),
        checkNestedMount);
  }

  /**
   * Unmounts the given Alluxio inodes. The inodes should match an existing mount point. If
   * inodes is an empty list, delete will not update the MountTableTrie.
   *
   * @param journalContext journal context
   * @param inodes the target inodes
   * @param uri the alluxio uri of target LockedInodePath
   * @param checkNestedMount  whether to check nested mount points before delete
   * @return whether the operation succeeded or not
   */
  public boolean delete(Supplier<JournalContext> journalContext, List<InodeView> inodes,
                        AlluxioURI uri, boolean checkNestedMount) {
    String path = uri.getPath();
    LOG.info("Unmounting {}", path);
    if (path.equals(ROOT)) {
      LOG.warn("Cannot unmount the root mount point.");
      return false;
    }

    try (LockResource r = new LockResource(mWriteLock)) {
      if (mState.getMountTable().containsKey(path)) {
        // check if the path contains another nested mount point
        if (checkNestedMount) {
          for (String mountPath : mState.getMountTable().keySet()) {
            try {
              if (PathUtils.hasPrefix(mountPath, path) && (!path.equals(mountPath))) {
                LOG.warn("The path to unmount {} contains another nested mountpoint {}",
                    path, mountPath);
                return false;
              }
            } catch (InvalidPathException e) {
              LOG.warn("Invalid path {} encountered when checking for nested mount point", path);
            }
          }
        }
        MountInfo info = mState.getMountTable().get(path);
        mUfsManager.removeMount(info.getMountId());
        mUfsManager.removeMount(mState.getMountTable().get(path).getMountId());
        // TODO(Jiadong): alter this interface
        if (mState.getMountTableTrie().isEnabled() && !inodes.isEmpty()) {
          mState.getMountTableTrie().removeMountPoint(inodes);
        }
        mState.applyAndJournal(journalContext,
            DeleteMountPointEntry.newBuilder().setAlluxioPath(path).build());
        return true;
      }
      LOG.warn("Mount point {} does not exist.", path);
      return false;
    }
  }

  /**
   * Update the mount point with new options and mount ID.
   *
   * @param journalContext the journal context
   * @param alluxioLockedInodePath an Alluxio LockedInodePath
   * @param newMountId the mount id
   * @param newOptions the mount options
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public void update(Supplier<JournalContext> journalContext,
                     LockedInodePath alluxioLockedInodePath,
                     long newMountId, MountPOptions newOptions) throws InvalidPathException,
      FileAlreadyExistsException, IOException {
    AlluxioURI alluxioUri = alluxioLockedInodePath.getUri();
    try (LockResource r = new LockResource(mWriteLock)) {
      MountInfo mountInfo = getMountTable().get(alluxioUri.getPath());
      if (mountInfo == null || !delete(journalContext, alluxioLockedInodePath, false)) {
        throw new InvalidPathException(String.format("Failed to update mount point at %s."
            + " Please ensure the path is an existing mount point and not root.",
            alluxioUri.getPath()));
      }
      try {
        add(journalContext, alluxioLockedInodePath, mountInfo.getUfsUri(), newMountId, newOptions);
      } catch (FileAlreadyExistsException | InvalidPathException | IOException e) {
        // This should never happen since the path is guaranteed to exist and the mount point is
        // just removed from the same path.
        LOG.error("Failed to add the updated mount point at {}", alluxioUri, e);
        // re-add old mount point
        add(journalContext, alluxioLockedInodePath, mountInfo.getUfsUri(),
            mountInfo.getMountId());
        throw e;
      }
    }
  }

  /**
   * Returns the closest ancestor mount point the given path is nested under.
   *
   * @param alluxioLockedInodePath an Alluxio LockedInodePath
   * @return mount point the given Alluxio path is nested under
   * @throws InvalidPathException if an invalid path is encountered
   */
  public String getMountPoint(LockedInodePath alluxioLockedInodePath) throws InvalidPathException {
    try (LockResource r = new LockResource(mReadLock)) {
      // if MountTableTrie is enabled and inodeViewList is a non-empty list, use the Trie
      // version of getMountPoint.
      if (mState.getMountTableTrie().isEnabled() && alluxioLockedInodePath.fullPathExists()) {
        return mState.getMountTableTrie().getMountPoint(alluxioLockedInodePath.getInodeViewList());
      } else {
        return getMountPoint(alluxioLockedInodePath.getUri());
      }
    }
  }

  /**
   * Returns the closest ancestor mount point the given path is nested under.
   *
   * @param uri an Alluxio uri
   * @return the mountpoint if given uri
   * @throws InvalidPathException
   */
  public String getMountPoint(AlluxioURI uri) throws InvalidPathException {
    try (LockResource r = new LockResource(mReadLock)) {
      String path = uri.getPath();
      String lastMount = ROOT;
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String mount = entry.getKey();
        // we choose a new candidate path if the previous candidate path is a prefix
        // of the current alluxioPath and the alluxioPath is a prefix of the path.
        if (!mount.equals(ROOT) && PathUtils.hasPrefix(path, mount)
            && PathUtils.hasPrefix(mount, lastMount)) {
          lastMount = mount;
        }
      }
      return lastMount;
    }
  }

  /**
   * Returns a copy of the current mount table, the mount table is a map from Alluxio file system
   * URIs to the corresponding mount point information.
   *
   * @return a copy of the current mount table
   */
  public Map<String, MountInfo> getMountTable() {
    try (LockResource r = new LockResource(mReadLock)) {
      return new HashMap<>(mState.getMountTable());
    }
  }

  /**
   * Enable the MountTableTrie based on given InodeTree. This will trigger the recovering of
   * MountTableTrie.
   *
   * @param inodeTree the rootInode set in MountTableTrie
   */
  public void enableMountTableTrie(InodeTree inodeTree) throws Exception {
    try (LockResource r = new LockResource(mWriteLock)) {
      recoverMountTableTrie(inodeTree);
    }
  }

  /**
   * Enable the MountTableTrie based on the given rootInode. It will not trigger the recovering
   * of MountTableTrie.
   * @param rootInode the rootInode set in MountTableTrie
   */
  public void enableMountTableTrie(InodeView rootInode) {
    try (LockResource r = new LockResource(mWriteLock)) {
      mState.getMountTableTrie().setRootInode(rootInode);
    }
  }

  /**
   * Check if the given lockedInodePath has a descendant which is a mount point. It will call
   * {@link MountTable#containsMountPoint(List, AlluxioURI, boolean)}.
   *
   * @param alluxioLockedInodePath an Alluxio LockedInodePath
   * @param containsSelf cause method to return true when given uri itself is a mount point
   *
   * @return true if the given uri has a descendant which is a mount point [, or is a mount point]
   */
  public boolean containsMountPoint(LockedInodePath alluxioLockedInodePath, boolean containsSelf)
      throws InvalidPathException {
    Preconditions.checkArgument(alluxioLockedInodePath.fullPathExists());
    return containsMountPoint(alluxioLockedInodePath.getInodeViewList(),
        alluxioLockedInodePath.getUri(), containsSelf);
  }

  /**
   * Check if the given Alluxio inodes and Alluxio uri have a descendant which is a mount point.
   * It will check the state of MountTableTrie and whether the inodes list is empty.
   * @param alluxioInodes the target Alluxio inodes
   * @param uri the target Alluxio uri
   * @param containsSelf cause method to return true when given uri itself is a mount point
   * @return true if the given uri has a descendant which is a mount point [, or is a mount point]
   * @throws InvalidPathException could be thrown by hasPrefix
   */
  private boolean containsMountPoint(List<InodeView> alluxioInodes, AlluxioURI uri,
      boolean containsSelf) throws InvalidPathException {
    try (LockResource r = new LockResource(mReadLock)) {
      if (mState.getMountTableTrie().isEnabled() && !alluxioInodes.isEmpty()) {
        return mState.getMountTableTrie().hasChildrenContainsMountPoints(alluxioInodes,
            containsSelf);
      } else {
        String path = uri.getPath();
        for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
          String mountPath = entry.getKey();
          if (!containsSelf && mountPath.equals(path)) {
            continue;
          }
          if (PathUtils.hasPrefix(mountPath, path)) {
            return true;
          }
        }
        return false;
      }
    }
  }

  /**
   * Returns the mount points under the specified path.
   *
   * @param alluxioLockedInodePath an Alluxio LockedInodePath
   * @param containsSelf if the given uri itself can be a mount point and included in the return
   * @return the mount points found
   */
  public List<MountInfo> findChildrenMountPoints(LockedInodePath alluxioLockedInodePath,
      boolean containsSelf) throws InvalidPathException {
    AlluxioURI uri = alluxioLockedInodePath.getUri();
    String path = uri.getPath();
    List<MountInfo> childrenMountPoints = new ArrayList<>();

    try (LockResource r = new LockResource(mReadLock)) {
      // Here, we can guarantee that the alluxioLockedInodePath is fullPathExists.
      if (mState.getMountTableTrie().isEnabled()) {
        List<String> mountPointsPath =
            mState.getMountTableTrie().findChildrenMountPoints(alluxioLockedInodePath,
                containsSelf);
        for(String p : mountPointsPath) {
          childrenMountPoints.add(mState.getMountTable().get(p));
        }
      } else {
        for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
          String mountPath = entry.getKey();
          if (!containsSelf && mountPath.equals(path)) {
            continue;
          }
          if (PathUtils.hasPrefix(mountPath, path)) {
            childrenMountPoints.add(entry.getValue());
          }
        }
      }
    }
    return childrenMountPoints;
  }

  /**
   * @param uri an Alluxio path URI
   * @return whether the given uri is a mount point
   */
  public boolean isMountPoint(AlluxioURI uri) {
    try (LockResource r = new LockResource(mReadLock)) {
      return mState.getMountTable().containsKey(uri.getPath());
    }
  }

  private static AlluxioURI reverseResolve(AlluxioURI mountPoint,
      AlluxioURI ufsUriMountPoint, AlluxioURI ufsUri)
      throws InvalidPathException {
    String relativePath = PathUtils.subtractPaths(
        PathUtils.normalizePath(ufsUri.getPath(), AlluxioURI.SEPARATOR),
        PathUtils.normalizePath(ufsUriMountPoint.getPath(), AlluxioURI.SEPARATOR));
    if (relativePath.isEmpty()) {
      return mountPoint;
    } else {
      return mountPoint.joinUnsafe(relativePath);
    }
  }

  /**
   * Resolves the given Ufs path. If the given UFs path is mounted in Alluxio space, it returns
   * the associated Alluxio path.
   * @param ufsUri an Ufs path URI
   * @return an Alluxio path URI
   */
  @Nullable
  public ReverseResolution reverseResolve(AlluxioURI ufsUri) {
    // TODO(ggezer): Consider alternative mount table representations for optimizing this method.
    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> mountInfoEntry : mState.getMountTable().entrySet()) {
        try {
          if (mountInfoEntry.getValue().getUfsUri().isAncestorOf(ufsUri)) {
            return new ReverseResolution(mountInfoEntry.getValue(),
                reverseResolve(mountInfoEntry.getValue().getAlluxioUri(),
                    mountInfoEntry.getValue().getUfsUri(), ufsUri));
          }
        } catch (InvalidPathException e) {
          // expected when ufsUri does not belong to this particular mountPoint
          LOG.debug(Throwables.getStackTraceAsString(e));
        }
      }
      return null;
    }
  }

  /**
   * Get the associated ufs client with the mount id.
   * @param mountId mount id to look up ufs client
   * @return ufsClient
   */
  @Nullable
  public UfsManager.UfsClient getUfsClient(long mountId) {
    try {
      return mUfsManager.get(mountId);
    } catch (NotFoundException | UnavailableException e) {
      LOG.warn("failed to get ufsclient for mountid {}, exception {}", mountId, e);
    }
    return null;
  }

  /**
   * Resolves the given Alluxio path. If the given Alluxio path is nested under a mount point, the
   * resolution maps the Alluxio path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op. It will call {@link MountTable#resolve(AlluxioURI, List)}.
   *
   * @param alluxioLockedInodePath an Alluxio LockedInodePath
   * @return the {@link Resolution} representing the UFS path
   * @throws InvalidPathException if an invalid path is encountered
   */
  public Resolution resolve(LockedInodePath alluxioLockedInodePath) throws InvalidPathException {
    if (alluxioLockedInodePath.fullPathExists()) {
      return resolve(alluxioLockedInodePath.getUri(), alluxioLockedInodePath.getInodeViewList());
    }
    return resolve(alluxioLockedInodePath.getUri());
  }

  // TODO(Jiadong): should we remove it? code duplication here
  /**
   * Resolves the given Alluxio path. If the given Alluxio path is nested under a mount point, the
   * resolution maps the Alluxio path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op. It will call {@link MountTable#resolve(AlluxioURI, List)}.
   *
   * @param uri an Alluxio LockedInodePath
   * @return the {@link Resolution} representing the UFS path
   * @throws InvalidPathException if an invalid path is encountered
   */
  public Resolution resolve(AlluxioURI uri) throws InvalidPathException {
    try (LockResource r = new LockResource(mReadLock)) {
      String path = uri.getPath();
      LOG.debug("Resolving {}", path);
      PathUtils.validatePath(uri.getPath());
      // This will re-acquire the read lock, but that is allowed.
      String mountPoint = getMountPoint(uri);
      // TODO(Jiadong): figure out how we can remove this duplication
      if (mountPoint != null) {
        MountInfo info = mState.getMountTable().get(mountPoint);
        AlluxioURI ufsUri = info.getUfsUri();
        UfsManager.UfsClient ufsClient;
        AlluxioURI resolvedUri;
        try {
          ufsClient = mUfsManager.get(info.getMountId());
          try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
            UnderFileSystem ufs = ufsResource.get();
            resolvedUri = ufs.resolveUri(ufsUri, path.substring(mountPoint.length()));
          }
        } catch (NotFoundException | UnavailableException e) {
          throw new RuntimeException(
              String.format("No UFS information for %s for mount Id %d, we should never reach here",
                  uri, info.getMountId()), e);
        }
        return new Resolution(resolvedUri, ufsClient, info.getOptions().getShared(),
            info.getMountId());
      }
      // TODO(binfan): throw exception as we should never reach here
      return new Resolution(uri, null, false, IdUtils.INVALID_MOUNT_ID);
    }
  }

  /**
   * Underlying implementation of resolve. If the calling context has no LockedInodePath, then
   * call this method and pass an empty array list as the second parameter.
   * @param uri target alluxio path's uri
   * @param inodeViewList target alluxio path's inodes
   * @return the {@link Resolution} representing the UFS path
   * @throws InvalidPathException if an invalid path is encountered
   */
  public Resolution resolve(AlluxioURI uri, List<InodeView> inodeViewList)
      throws InvalidPathException {
    try (LockResource r = new LockResource(mReadLock)) {
      String path = uri.getPath();
      LOG.debug("Resolving {}", path);
      PathUtils.validatePath(uri.getPath());
      // This will re-acquire the read lock, but that is allowed.
      String mountPoint;
      if (inodeViewList.isEmpty()) {
        mountPoint = getMountPoint(uri);
      } else {
        mountPoint = mState.getMountTableTrie().getMountPoint(inodeViewList);
      }
      if (mountPoint != null) {
        MountInfo info = mState.getMountTable().get(mountPoint);
        AlluxioURI ufsUri = info.getUfsUri();
        UfsManager.UfsClient ufsClient;
        AlluxioURI resolvedUri;
        try {
          ufsClient = mUfsManager.get(info.getMountId());
          try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
            UnderFileSystem ufs = ufsResource.get();
            resolvedUri = ufs.resolveUri(ufsUri, path.substring(mountPoint.length()));
          }
        } catch (NotFoundException | UnavailableException e) {
          throw new RuntimeException(
              String.format("No UFS information for %s for mount Id %d, we should never reach here",
                  uri, info.getMountId()), e);
        }
        return new Resolution(resolvedUri, ufsClient, info.getOptions().getShared(),
            info.getMountId());
      }
      // TODO(binfan): throw exception as we should never reach here
      return new Resolution(uri, null, false, IdUtils.INVALID_MOUNT_ID);
    }
  }

  /**
   * Checks to see if a write operation is allowed for the specified Alluxio path, by determining
   * if it is under a readonly mount point.
   *
   * @param alluxioLockedInodePath an Alluxio LockedInodePath
   * @throws InvalidPathException if the Alluxio path is invalid
   * @throws AccessControlException if the Alluxio path is under a readonly mount point
   */
  public void checkUnderWritableMountPoint(LockedInodePath alluxioLockedInodePath)
      throws InvalidPathException, AccessControlException {
    try (LockResource r = new LockResource(mReadLock)) {
      // This will re-acquire the read lock, but that is allowed.
      String mountPoint = getMountPoint(alluxioLockedInodePath);
      MountInfo mountInfo = mState.getMountTable().get(mountPoint);
      if (mountInfo.getOptions().getReadOnly()) {
        throw new AccessControlException(ExceptionMessage.MOUNT_READONLY,
            alluxioLockedInodePath.getUri()
            , mountPoint);
      }
    }
  }

  /**
   * @param mountId the given ufs id
   * @return the mount information with this id or null if this mount id is not found
   */
  @Nullable
  public MountInfo getMountInfo(long mountId) {
    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        if (entry.getValue().getMountId() == mountId) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  /**
   * Recover MountTableTrie based on InodeTree.
   * @param inodeTree the initialized inodeTree
   */
  public void recoverMountTableTrie(InodeTree inodeTree) throws Exception {
    try (LockResource r = new LockResource(mWriteLock)) {
      mState.getMountTableTrie().recoverFromInodeTreeAndMountPoints(inodeTree,
          mState.getMountTable().keySet());
    }
  }

  /**
   * Gets mount information for the path.
   * @param uri the path
   * @return the mount information
   */
  public MountInfo getMountInfo(AlluxioURI uri) throws InvalidPathException {
    try (LockResource ignored = new LockResource(mReadLock)) {
      String path = uri.getPath();
      LOG.debug("Resolving {}", path);
      PathUtils.validatePath(uri.getPath());
      // This will re-acquire the read lock, but that is allowed.
      String mountPoint = getMountPoint(uri);
      if (mountPoint != null) {
        return mState.getMountTable().get(mountPoint);
      }
    }
    throw new IllegalStateException("No mount found for path " + uri);
  }

  /**
   * @return the invalidation sync cache
   */
  public UfsSyncPathCache getUfsSyncPathCache() {
    return mState.mUfsSyncPathCache;
  }

  @Override
  public Journaled getDelegate() {
    return mState;
  }

  /**
   * Creates a mount point ID and guarantees uniqueness.
   *
   * @return the mount point ID
   */
  public long createUnusedMountId() {
    long mountId = IdUtils.createMountId();
    while (mUfsManager.hasMount(mountId)) {
      LOG.debug("IdUtils generated an duplicated mountId {}, generate another one.", mountId);
      mountId = IdUtils.createMountId();
    }
    return mountId;
  }

  /**
   * This class represents a UFS path after resolution. The UFS URI and the {@link UnderFileSystem}
   * for the UFS path are available.
   */
  public static final class Resolution {
    private final AlluxioURI mUri;
    private final UfsManager.UfsClient mUfsClient;
    private final boolean mShared;
    private final long mMountId;

    private Resolution(AlluxioURI uri, UfsManager.UfsClient ufs, boolean shared, long mountId) {
      mUri = uri;
      mUfsClient = ufs;
      mShared = shared;
      mMountId = mountId;
    }

    /**
     * @return the URI in the ufs
     */
    public AlluxioURI getUri() {
      return mUri;
    }

    /**
     * @return the mount uri in the ufs
     */
    public AlluxioURI getUfsMountPointUri() {
      return mUfsClient.getUfsMountPointUri();
    }

    /**
     * @return the {@link UnderFileSystem} closeable resource
     */
    public CloseableResource<UnderFileSystem> acquireUfsResource() {
      return mUfsClient.acquireUfsResource();
    }

    /**
     * @return the shared option
     */
    public boolean getShared() {
      return mShared;
    }

    /**
     * @return the id of this mount point
     */
    public long getMountId() {
      return mMountId;
    }

    /**
     * @return the ufsClient corresponding to this ufs
     */
    public UfsManager.UfsClient getUfsClient() {
      return mUfsClient;
    }
  }

  /**
   * This class represents an Alluxio path after reverse resolution.
   */
  public static final class ReverseResolution {
    private final MountInfo mMountInfo;
    private final AlluxioURI mUri;

    private ReverseResolution(MountInfo mountInfo, AlluxioURI uri) {
      mMountInfo = mountInfo;
      mUri = uri;
    }

    /**
     * @return the URI in the Alluxio
     */
    public AlluxioURI getUri() {
      return mUri;
    }

    /**
     * @return the {@link MountInfo} that resolved the URI
     */
    public MountInfo getMountInfo() {
      return mMountInfo;
    }
  }

  /**
   * Operator for MountTableTrie traverse.
   */
  public static final class MountTableTrieOperator {

    /**
     * Substitute the EmptyInode with InodeView.
     * @param trieNode the target TrieNode
     * @param inode the target InodeView
     * @return whether substitution happens
     */
    public static boolean checkAndSubstitute(TrieNode<InodeView> trieNode, InodeView inode) {
      // return true if the substitution happens
      if (!(inode instanceof EmptyInode)) {
        Collection<InodeView> targetChildInodes = trieNode.childrenKeys();
        // Traverse the children of current TrieNode, see if there is any existing EmptyInode
        // that presents the same inode with inode, substitute it and return true
        for (InodeView existingInode : targetChildInodes) {
          if (existingInode instanceof EmptyInode && existingInode.equals(inode)) {
            // acquire the corresponding trieNode
            TrieNode<InodeView> targetTrieNode = trieNode.child(existingInode);
            // remove the existing <EmptyInode, TrieNode> pair
            trieNode.removeChild(existingInode);
            // add the <InodeView, TrieNode>
            trieNode.addChild(inode, targetTrieNode);
            return true;
          }
        }
      }
      return false;
    }
  }

  /**
   * Helper function to generate MountInfo.
   */
  static MountInfo fromAddMountPointEntry(AddMountPointEntry entry) {
    return
        new MountInfo(new AlluxioURI(entry.getAlluxioPath()), new AlluxioURI(entry.getUfsPath()),
            entry.getMountId(), GrpcUtils.fromMountEntry(entry));
  }

  /**
   * MountTableTrie encapsulates some basic operations of TrieNode.
   */
  public static final class MountTableTrie {
    // The root of Trie of current MountTable
    private TrieNode<InodeView> mRootTrieNode;
    // Map from TrieNode to the alluxio path literal
    private Map<TrieNode<InodeView>, String> mMountPointTrieTable;

    private AtomicBoolean mIsMountTableTrieEnabled = new AtomicBoolean(false);

    /**
     * Constructor of MountTableTrie.
     */
    public MountTableTrie() {
      mRootTrieNode = new TrieNode<>();
      mMountPointTrieTable = new HashMap<>(10);
    }

    /**
     * Insert the root inode into Trie, and the MountTableTrie will be enabled.
     * @param rootInode the target root inode
     */
    public void setRootInode(InodeView rootInode) {
      Preconditions.checkNotNull(mRootTrieNode);
      Preconditions.checkNotNull(mMountPointTrieTable);
      Preconditions.checkArgument(mRootTrieNode.isLastTrieNode());

      TrieNode<InodeView> rootTrieInode =
          mRootTrieNode.insert(Collections.singletonList(rootInode));
      mMountPointTrieTable.put(rootTrieInode, ROOT);
      enable();
    }

    /**
     * Rebuilds the MountTableTrie from inodeTree and existing mount points.
     * @param inodeTree the given inodeTree
     * @param mountPoints the existing mountPoints
     * @throws InvalidPathException
     */
    public void recoverFromInodeTreeAndMountPoints(InodeTree inodeTree,
        Set<String> mountPoints) throws Exception {
      Preconditions.checkNotNull(inodeTree);
      Preconditions.checkNotNull(mountPoints);
      Preconditions.checkNotNull(inodeTree.getRoot());
      mRootTrieNode = new TrieNode<>();
      mMountPointTrieTable = new HashMap<>(10);
      for (String mountPoint : mountPoints) {
        List<InodeView> inodeViews = inodeTree.getInodesByPath(mountPoint);
        addMountPointInternal(mountPoint, inodeViews);
      }
      enable();
    }

    /**
     * Reset the MountTableTrie, this will disable the Trie and clear the data inside.
     */
    public void disable() {
      mIsMountTableTrieEnabled.set(false);
      mMountPointTrieTable.clear();
      mRootTrieNode = new TrieNode<>();
    }

    /**
     * Enable the MountTableTrie, this implicity means that the data inside MountTableTrie is now
     * up-to-date; only when MountTableTrie is DISABLE or PENDING can its state be set to ENABLED.
     */
    public void enable() {
//      Preconditions.checkArgument(isDisabled());
      mIsMountTableTrieEnabled.set(true);
    }

    private void addMountPointInternal(String mountPoint, List<InodeView> inodeViews) {
      if (!inodeViews.isEmpty()) {
        TrieNode<InodeView> node = mRootTrieNode.insert(inodeViews,
            MountTableTrieOperator::checkAndSubstitute);
        mMountPointTrieTable.put(node, mountPoint);
      }
    }

    /**
     * Insert the given inodes into the MountTableTrie.
     * @param uri the alluxioUri of the target inodes
     * @param inodes the inodes of target inodePath
     */
    public void addMountPoint(AlluxioURI uri, List<InodeView> inodes) {
      Preconditions.checkArgument(inodes != null && !inodes.isEmpty());
      Preconditions.checkArgument(isEnabled());
      Preconditions.checkNotNull(mRootTrieNode);
      addMountPointInternal(uri.getPath(), inodes);
    }

    /**
     * Remove a TrieNode from MountTableTrie based on the lockedInodePath.
     * @param inodes the target list of inodes
     */
    public void removeMountPoint(List<InodeView> inodes) {
      Preconditions.checkArgument(inodes != null && !inodes.isEmpty());
      Preconditions.checkArgument(isEnabled());
      Preconditions.checkNotNull(mRootTrieNode);
      TrieNode<InodeView> trieNode =
          mRootTrieNode.remove(inodes);
      mMountPointTrieTable.remove(trieNode);
    }

    /**
     * Get the mount point of the given inodes.
     * @param inodeViewList the target inodes
     * @return the lowest mountPoint of the given path
     */
    public String getMountPoint(List<InodeView> inodeViewList) {
      Preconditions.checkArgument(isEnabled());
      Preconditions.checkNotNull(mRootTrieNode);

      TrieNode<InodeView> res =
          mRootTrieNode.lowestMatchedTrieNode(inodeViewList, true,
              MountTableTrieOperator::checkAndSubstitute, false);
      return mMountPointTrieTable.get(res);
    }

    /**
     * Finds all the mount point among the children TrieNodes of the given path. It will call
     * {@link MountTableTrie#findChildrenMountPoints(List, boolean)}.
     * @param path the target inodePath
     * @param isContainSelf true if the results can contain the TrieNode of the given path
     * @return the qualified children mount points of the target path
     */
    public List<String> findChildrenMountPoints(LockedInodePath path, boolean isContainSelf) {
      Preconditions.checkArgument(isEnabled());
      Preconditions.checkArgument(path.fullPathExists());
      return findChildrenMountPoints(path.getInodeViewList(), isContainSelf);
    }

    /**
     * Finds all the mount point among the children TrieNodes of the given inodes.
     * @param inodeViewList the target inodes
     * @param isContainSelf true if the results can contain the TrieNode of the given path
     * @return the qualified children mount points of the target path
     */
    public List<String> findChildrenMountPoints(List<InodeView> inodeViewList,
                                                boolean isContainSelf) {
      Preconditions.checkArgument(inodeViewList != null && !inodeViewList.isEmpty());
      Preconditions.checkArgument(isEnabled());
      Preconditions.checkNotNull(mRootTrieNode);

      List<String> mountPoints = new ArrayList<>();
      TrieNode<InodeView> trieNode =
          mRootTrieNode.lowestMatchedTrieNode(inodeViewList, false,
              MountTableTrieOperator::checkAndSubstitute, true);
      if (trieNode == null) {
        return mountPoints;
      }
      List<TrieNode<InodeView>> childrenTrieNodes =
          trieNode.descendants(true, isContainSelf);
      for (TrieNode<InodeView> node : childrenTrieNodes) {
        mountPoints.add(mMountPointTrieTable.get(node));
      }
      return mountPoints;
    }

    /**
     * Checks if the given inodes contains children paths that are mountPoint.
     * @param inodeViewList the target inodes
     * @param isContainSelf true if the search targets will include the given path
     * @return true if the target inodes contains at least one mountPoint
     */
    public boolean hasChildrenContainsMountPoints(List<InodeView> inodeViewList,
                                                  boolean isContainSelf) {
      Preconditions.checkArgument(isEnabled());
      Preconditions.checkNotNull(mRootTrieNode);
      TrieNode<InodeView> trieNode =
          mRootTrieNode.lowestMatchedTrieNode(inodeViewList, false,
              MountTableTrieOperator::checkAndSubstitute, true);
      if (trieNode == null) {
          mRootTrieNode.lowestMatchedTrieNode(inodeViewList, n -> true, true);
      if(trieNode == null) {
        return false;
      }
      return trieNode.hasNestedTerminalTrieNodes(isContainSelf);
    }

    /**
     * Checks if MountTableTrie is ENABLED.
     * @return true if the MountTableTrie is ENABLED
     */
    public boolean isEnabled() {
      return mIsMountTableTrieEnabled.get();
    }
  }

  /**
   * Persistent mount table state. replayJournalEntryFromJournal should only be called during
   * journal replay. To modify the mount table, create a journal entry and call one of the
   * applyAndJournal methods.
   */
  @ThreadSafe
  public final class State implements Journaled {
    /**
     * Map from Alluxio path string to mount info.
     */
    private final Map<String, MountInfo> mMountTable;
    /** Map from mount id to cache of paths which have been synced with UFS. */
    private final UfsSyncPathCache mUfsSyncPathCache;

    private final MountTableTrie mMountTableTrie;

    /**
     * @param mountInfo root mount info
     * @param clock the clock used for computing sync times
     */
    State(MountInfo mountInfo, Clock clock) {
      mMountTable = new HashMap<>(10);
      mMountTable.put(MountTable.ROOT, mountInfo);
      mMountTableTrie = new MountTableTrie();
      mUfsSyncPathCache = new UfsSyncPathCache(clock);
    }

    /**
     * @return an unmodifiable view of the mount table
     */
    public Map<String, MountInfo> getMountTable() {
      return Collections.unmodifiableMap(mMountTable);
    }

    /**
     * @return the MountTableTrie object
     */
    public MountTableTrie getMountTableTrie() {
      return mMountTableTrie;
    }

    /**
     * @param context journal context
     * @param entry add mount point entry
     */
    public void applyAndJournal(Supplier<JournalContext> context, AddMountPointEntry entry) {
      applyAndJournal(context, JournalEntry.newBuilder().setAddMountPoint(entry).build());
    }

    /**
     * @param context journal context
     * @param entry delete mount point entry
     */
    public void applyAndJournal(Supplier<JournalContext> context, DeleteMountPointEntry entry) {
      applyAndJournal(context, JournalEntry.newBuilder().setDeleteMountPoint(entry).build());
    }

    private void applyAddMountPoint(AddMountPointEntry entry) {
      try (LockResource r = new LockResource(mWriteLock)) {
        MountInfo mountInfo = fromAddMountPointEntry(entry);
        mMountTable.put(entry.getAlluxioPath(), mountInfo);
      }
    }

    private void applyDeleteMountPoint(DeleteMountPointEntry entry) {
      try (LockResource r = new LockResource(mWriteLock)) {
        mMountTable.remove(entry.getAlluxioPath());
      }
    }

    @Override
    public boolean processJournalEntry(JournalEntry entry) {
      if (entry.hasAddMountPoint()) {
        applyAddMountPoint(entry.getAddMountPoint());
      } else if (entry.hasDeleteMountPoint()) {
        applyDeleteMountPoint(entry.getDeleteMountPoint());
      } else {
        return false;
      }
      return true;
    }

    @Override
    public void resetState() {
      try (LockResource r = new LockResource(mWriteLock)) {
        MountInfo mountInfo = mMountTable.get(ROOT);
        mMountTable.clear();
        if (mountInfo != null) {
          mMountTable.put(ROOT, mountInfo);
        }
      }
      if (mMountTableTrie.isEnabled()) {
        mMountTableTrie.disable();
      }
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
      try (LockResource r = new LockResource(mReadLock)) {
        return getJournalEntryIteratorInternal();
      }
    }

    private CloseableIterator<JournalEntry> getJournalEntryIteratorInternal() {
      final Iterator<Map.Entry<String, MountInfo>> it = mMountTable.entrySet().iterator();
      return CloseableIterator.noopCloseable(new Iterator<Journal.JournalEntry>() {
        /** mEntry is always set to the next non-root mount point if exists. */
        private Map.Entry<String, MountInfo> mEntry = null;

        @Override
        public boolean hasNext() {
          if (mEntry != null) {
            return true;
          }
          if (it.hasNext()) {
            mEntry = it.next();
            // Skip the root mount point, which is considered a part of initial state, not journaled
            // state.
            if (mEntry.getKey().equals(ROOT)) {
              mEntry = null;
              return hasNext();
            }
            return true;
          }
          return false;
        }

        @Override
        public Journal.JournalEntry next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          String alluxioPath = mEntry.getKey();
          MountInfo info = mEntry.getValue();
          mEntry = null;

          Map<String, String> properties = info.getOptions().getProperties();
          List<File.StringPairEntry> protoProperties = new ArrayList<>(properties.size());
          for (Map.Entry<String, String> property : properties.entrySet()) {
            protoProperties.add(File.StringPairEntry.newBuilder()
                .setKey(property.getKey())
                .setValue(property.getValue())
                .build());
          }

          AddMountPointEntry addMountPoint =
              AddMountPointEntry.newBuilder().setAlluxioPath(alluxioPath)
                  .setMountId(info.getMountId()).setUfsPath(info.getUfsUri().toString())
                  .setReadOnly(info.getOptions().getReadOnly()).addAllProperties(protoProperties)
                  .setShared(info.getOptions().getShared()).build();
          return Journal.JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Mountable#Iterator#remove is not supported.");
        }
      });
    }

    @Override
    public CheckpointName getCheckpointName() {
      return CheckpointName.MOUNT_TABLE;
    }
  }
}
