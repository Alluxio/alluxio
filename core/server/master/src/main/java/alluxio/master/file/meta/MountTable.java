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
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
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

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
  private final State mState;

  /** The manager of all ufs. */
  private final UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link MountTable}.
   *
   * @param ufsManager the UFS manager
   * @param rootMountInfo root mount info
   */
  public MountTable(UfsManager ufsManager, MountInfo rootMountInfo) {
    mState = new State(rootMountInfo);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    mReadLock = lock.readLock();
    mWriteLock = lock.writeLock();
    mUfsManager = ufsManager;
  }

  /**
   * Mounts the given UFS path at the given Alluxio path. The Alluxio path should not be nested
   * under an existing mount point.
   *
   * @param journalContext the journal context
   * @param alluxioUri an Alluxio path URI
   * @param ufsUri a UFS path URI
   * @param mountId the mount id
   * @param options the mount options
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public void add(Supplier<JournalContext> journalContext, AlluxioURI alluxioUri, AlluxioURI ufsUri,
      long mountId, MountPOptions options) throws FileAlreadyExistsException, InvalidPathException {
    String alluxioPath = alluxioUri.getPath().isEmpty() ? "/" : alluxioUri.getPath();
    LOG.info("Mounting {} at {}", ufsUri, alluxioPath);

    try (LockResource r = new LockResource(mWriteLock)) {
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

      Map<String, String> properties = options.getPropertiesMap();
      mState.applyAndJournal(journalContext, AddMountPointEntry.newBuilder()
          .addAllProperties(properties.entrySet().stream()
              .map(entry -> StringPairEntry.newBuilder()
                  .setKey(entry.getKey()).setValue(entry.getValue()).build())
              .collect(Collectors.toList()))
          .setAlluxioPath(alluxioPath)
          .setMountId(mountId)
          .setReadOnly(options.getReadOnly())
          .setShared(options.getShared())
          .setUfsPath(ufsUri.toString())
          .build());
    }
  }

  /**
   * Unmounts the given Alluxio path. The path should match an existing mount point.
   *
   * @param journalContext journal context
   * @param uri an Alluxio path URI
   * @param checkNestedMount whether to check nested mount points before delete
   * @return whether the operation succeeded or not
   */
  public boolean delete(Supplier<JournalContext> journalContext, AlluxioURI uri,
      boolean checkNestedMount) {
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
        mUfsManager.removeMount(mState.getMountTable().get(path).getMountId());
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
   * @param alluxioUri an Alluxio path URI
   * @param newMountId the mount id
   * @param newOptions the mount options
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public void update(Supplier<JournalContext> journalContext, AlluxioURI alluxioUri,
      long newMountId, MountPOptions newOptions) throws InvalidPathException,
      FileAlreadyExistsException {
    try (LockResource r = new LockResource(mWriteLock)) {
      MountInfo mountInfo = getMountTable().get(alluxioUri.getPath());
      if (mountInfo == null || !delete(journalContext, alluxioUri, false)) {
        throw new InvalidPathException(String.format("Failed to update mount point at %s."
            + " Please ensure the path is an existing mount point and not root.",
            alluxioUri.getPath()));
      }
      try {
        add(journalContext, alluxioUri, mountInfo.getUfsUri(), newMountId, newOptions);
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        // This should never happen since the path is guaranteed to exist and the mount point is
        // just removed from the same path.
        LOG.error("Failed to add the updated mount point at {}", alluxioUri, e);
        // re-add old mount point
        add(journalContext, alluxioUri, mountInfo.getUfsUri(), mountInfo.getMountId(),
            mountInfo.getOptions());
        throw e;
      }
    }
  }

  /**
   * Returns the closest ancestor mount point the given path is nested under.
   *
   * @param uri an Alluxio path URI
   * @return mount point the given Alluxio path is nested under
   * @throws InvalidPathException if an invalid path is encountered
   */
  public String getMountPoint(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();
    String lastMount = ROOT;
    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String mount = entry.getKey();
        // we choose a new candidate path if the previous candidatepath is a prefix
        // of the current alluxioPath and the alluxioPath is a prefix of the path
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
   * @param uri the Alluxio uri to check
   * @param containsSelf cause method to return true when given uri itself is a mount point
   * @return true if the given uri has a descendant which is a mount point [, or is a mount point]
   */
  public boolean containsMountPoint(AlluxioURI uri, boolean containsSelf)
      throws InvalidPathException {
    String path = uri.getPath();

    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String mountPath = entry.getKey();
        if (!containsSelf && mountPath.equals(path)) {
          continue;
        }
        if (PathUtils.hasPrefix(mountPath, path)) {
          return true;
        }
      }
    }
    return false;
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

  private AlluxioURI reverseResolve(AlluxioURI mountPoint,
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
   * no-op.
   *
   * @param uri an Alluxio path URI
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
   * @param alluxioUri an Alluxio path URI
   * @throws InvalidPathException if the Alluxio path is invalid
   * @throws AccessControlException if the Alluxio path is under a readonly mount point
   */
  public void checkUnderWritableMountPoint(AlluxioURI alluxioUri)
      throws InvalidPathException, AccessControlException {
    try (LockResource r = new LockResource(mReadLock)) {
      // This will re-acquire the read lock, but that is allowed.
      String mountPoint = getMountPoint(alluxioUri);
      MountInfo mountInfo = mState.getMountTable().get(mountPoint);
      if (mountInfo.getOptions().getReadOnly()) {
        throw new AccessControlException(ExceptionMessage.MOUNT_READONLY, alluxioUri, mountPoint);
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

  @Override
  public Journaled getDelegate() {
    return mState;
  }

  /**
   * This class represents a UFS path after resolution. The UFS URI and the {@link UnderFileSystem}
   * for the UFS path are available.
   */
  public final class Resolution {
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
  }

  /**
   * This class represents a Alluxio path after reverse resolution.
   */
  public final class ReverseResolution {
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
   * Persistent mount table state. replayJournalEntryFromJournal should only be called during
   * journal replay. To modify the mount table, create a journal entry and call one of the
   * applyAndJournal methods.
   */
  public static final class State implements Journaled {
    /**
     * Map from Alluxio path string to mount info.
     */
    private final Map<String, MountInfo> mMountTable;

    /**
     * @param mountInfo root mount info
     */
    public State(MountInfo mountInfo) {
      mMountTable = new HashMap<>(10);
      mMountTable.put(MountTable.ROOT, mountInfo);
    }

    /**
     * @return an unmodifiable view of the mount table
     */
    public Map<String, MountInfo> getMountTable() {
      return Collections.unmodifiableMap(mMountTable);
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
      MountInfo mountInfo =
          new MountInfo(new AlluxioURI(entry.getAlluxioPath()), new AlluxioURI(entry.getUfsPath()),
              entry.getMountId(), GrpcUtils.fromMountEntry(entry));
      mMountTable.put(entry.getAlluxioPath(), mountInfo);
    }

    private void applyDeleteMountPoint(DeleteMountPointEntry entry) {
      mMountTable.remove(entry.getAlluxioPath());
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
      MountInfo mountInfo = mMountTable.get(ROOT);
      mMountTable.clear();
      if (mountInfo != null) {
        mMountTable.put(ROOT, mountInfo);
      }
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
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
