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
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.MountOptions;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.File.DeleteMountPointEntry;
import alluxio.proto.journal.File.StringPairEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;
import alluxio.util.io.PathUtils;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
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
public final class MountTable implements JournalEntryIterable, JournalEntryReplayable {
  private static final Logger LOG = LoggerFactory.getLogger(MountTable.class);

  public static final String ROOT = "/";

  private final Lock mReadLock;
  private final Lock mWriteLock;

  /** Mount table state that is preserved across restarts. */
  @GuardedBy("mReadLock,mWriteLock")
  private final State mState;

  /**
   * Creates a new instance of {@link MountTable}.
   *
   * @param rootMountInfo root mount info
   */
  public MountTable(MountInfo rootMountInfo) {
    mState = new State(rootMountInfo);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    mReadLock = lock.readLock();
    mWriteLock = lock.writeLock();
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    final Iterator<Map.Entry<String, MountInfo>> it = mState.getMountTable().entrySet().iterator();
    return new Iterator<Journal.JournalEntry>() {
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
                .setReadOnly(info.getOptions().isReadOnly()).addAllProperties(protoProperties)
                .setShared(info.getOptions().isShared()).build();
        return Journal.JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Mountable#Iterator#remove is not supported.");
      }
    };
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
      long mountId, MountOptions options) throws FileAlreadyExistsException, InvalidPathException {
    String alluxioPath = alluxioUri.getPath().isEmpty() ? "/" : alluxioUri.getPath();
    LOG.info("Mounting {} at {}", ufsUri, alluxioPath);

    try (LockResource r = new LockResource(mWriteLock)) {
      if (mState.getMountTable().containsKey(alluxioPath)) {
        throw new FileAlreadyExistsException(
            ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(alluxioPath));
      }
      // Check all non-root mount points, to check if they're a prefix of the alluxioPath we're
      // trying to mount. Also make sure that the ufs path we're trying to mount is not a prefix
      // or suffix of any existing mount path.
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String mountedAlluxioPath = entry.getKey();
        AlluxioURI mountedUfsUri = entry.getValue().getUfsUri();
        if (!mountedAlluxioPath.equals(ROOT)
            && PathUtils.hasPrefix(alluxioPath, mountedAlluxioPath)) {
          throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage(
              mountedAlluxioPath, alluxioPath));
        }
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
      mState.applyAndJournal(journalContext, AddMountPointEntry.newBuilder()
          .addAllProperties(options.getProperties().entrySet().stream()
              .map(entry -> StringPairEntry.newBuilder()
                  .setKey(entry.getKey()).setValue(entry.getValue()).build())
              .collect(Collectors.toList()))
          .setAlluxioPath(alluxioPath)
          .setMountId(mountId)
          .setReadOnly(options.isReadOnly())
          .setShared(options.isShared())
          .setUfsPath(ufsUri.toString())
          .build());
    }
  }

  /**
   * Clears all the mount points except the root.
   */
  public void reset() {
    LOG.info("Clearing mount table (except the root).");
    try (LockResource r = new LockResource(mWriteLock)) {
      mState.reset();
    }
  }

  /**
   * Unmounts the given Alluxio path. The path should match an existing mount point.
   *
   * @param journalContext journal context
   * @param uri an Alluxio path URI
   * @return whether the operation succeeded or not
   */
  public boolean delete(Supplier<JournalContext> journalContext, AlluxioURI uri) {
    String path = uri.getPath();
    LOG.info("Unmounting {}", path);
    if (path.equals(ROOT)) {
      LOG.warn("Cannot unmount the root mount point.");
      return false;
    }

    try (LockResource r = new LockResource(mWriteLock)) {
      if (mState.getMountTable().containsKey(path)) {
        mState.applyAndJournal(journalContext,
            DeleteMountPointEntry.newBuilder().setAlluxioPath(path).build());
        return true;
      }
      LOG.warn("Mount point {} does not exist.", path);
      return false;
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

    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String alluxioPath = entry.getKey();
        if (!alluxioPath.equals(ROOT) && PathUtils.hasPrefix(path, alluxioPath)) {
          return alluxioPath;
        }
      }
      return mState.getMountTable().containsKey(ROOT) ? ROOT : null;
    }
  }

  /**
   * Returns the mount info for the closest ancestor mount point.
   *
   * @param uri an Alluxio path URI
   * @return mount info for the mount point the given Alluxio path is nested under.
   */
  public MountInfo getMountInfo(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();

    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String alluxioPath = entry.getKey();
        if (!alluxioPath.equals(ROOT) && PathUtils.hasPrefix(path, alluxioPath)) {
          return entry.getValue();
        }
      }
      return mState.getMountTable().get(ROOT);
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
   * @return true if the specified uri is mount point, or has a descendant which is a mount point
   */
  public boolean containsMountPoint(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();

    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mState.getMountTable().entrySet()) {
        String mountPath = entry.getKey();
        if (PathUtils.hasPrefix(mountPath, path)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean containsUfsRoot(String root) {
    return mState.containsUfsRoot(root);
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
      if (mountInfo.getOptions().isReadOnly()) {
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
  public boolean replayJournalEntryFromJournal(JournalEntry entry) {
    return mState.replayJournalEntryFromJournal(entry);
  }

  /**
   * Persistent mount table state. replayJournalEntryFromJournal should only be called during
   * journal replay. To modify the mount table, create a journal entry and call one of the
   * applyAndJournal methods.
   */
  @ThreadSafe
  public static final class State implements JournalEntryReplayable {
    /** Maps from Alluxio path string, to {@link MountInfo}. */
    private final Map<String, MountInfo> mMountTable;
    private final Multiset<String> mUfsRoots;

    /**
     * @param mountInfo root mount info
     */
    public State(MountInfo mountInfo) {
      mMountTable = new ConcurrentHashMap<>(10);
      mUfsRoots = HashMultiset.create();

      addMount(ROOT, mountInfo);
    }

    /**
     * @return an unmodifiable view of the mount table
     */
    public Map<String, MountInfo> getMountTable() {
      return Collections.unmodifiableMap(mMountTable);
    }

    /**
     * @param root a ufs root path
     * @return whether the root exists in the mount table
     */
    public synchronized boolean containsUfsRoot(String root) {
      return mUfsRoots.contains(root);
    }

    @Override
    public synchronized boolean replayJournalEntryFromJournal(JournalEntry entry) {
      if (entry.hasAddMountPoint()) {
        apply(entry.getAddMountPoint());
      } else if (entry.hasDeleteMountPoint()) {
        apply(entry.getDeleteMountPoint());
      } else {
        return false;
      }
      return true;
    }

    /**
     * @param context journal context
     * @param entry add mount point entry
     */
    public synchronized void applyAndJournal(Supplier<JournalContext> context, AddMountPointEntry entry) {
      apply(entry);
      context.get().append(JournalEntry.newBuilder().setAddMountPoint(entry).build());
    }

    /**
     * @param context journal context
     * @param entry delete mount point entry
     */
    public synchronized void applyAndJournal(Supplier<JournalContext> context, DeleteMountPointEntry entry) {
      apply(entry);
      context.get().append(JournalEntry.newBuilder().setDeleteMountPoint(entry).build());
    }

    private void apply(AddMountPointEntry entry) {
      MountInfo mountInfo = new MountInfo(new AlluxioURI(entry.getAlluxioPath()),
          new AlluxioURI(entry.getUfsPath()), entry.getMountId(), new MountOptions(entry));
      addMount(entry.getAlluxioPath(), mountInfo);
    }

    private void apply(DeleteMountPointEntry entry) {
      AlluxioURI ufsUri = mMountTable.get(entry.getAlluxioPath()).getUfsUri();
      mUfsRoots.remove(getRoot(ufsUri));
      mMountTable.remove(entry.getAlluxioPath());
    }

    private String getRoot(AlluxioURI uri) {
      return new AlluxioURI(uri.getScheme(), uri.getAuthority(), "/").toString();
    }

    private void addMount(String alluxioUri, MountInfo mountInfo) {
      mUfsRoots.add(getRoot(mountInfo.getUfsUri()));
      mMountTable.put(alluxioUri, mountInfo);
    }

    /**
     * Resets the mount table state.
     */
    public synchronized void reset() {
      MountInfo mountInfo = mMountTable.get(ROOT);
      mUfsRoots.clear();
      mMountTable.clear();
      if (mountInfo != null) {
        addMount(ROOT, mountInfo);
      }
    }
  }
}
