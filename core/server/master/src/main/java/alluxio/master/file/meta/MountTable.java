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
import alluxio.master.journal.JournalCheckpointStreamable;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.Journal;
import alluxio.resource.LockResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is used for keeping track of Alluxio mount points.
 */
@ThreadSafe
public final class MountTable implements JournalCheckpointStreamable {
  private static final Logger LOG = LoggerFactory.getLogger(MountTable.class);

  public static final String ROOT = "/";

  private final ReentrantReadWriteLock mLock;
  private final Lock mReadLock;
  private final Lock mWriteLock;

  /** Maps from Alluxio path string, to {@link MountInfo}. */
  @GuardedBy("mLock")
  private final Map<String, MountInfo> mMountTable;

  /**
   * Creates a new instance of {@link MountTable}.
   */
  public MountTable() {
    final int initialCapacity = 10;
    mMountTable = new HashMap<>(initialCapacity);
    mLock = new ReentrantReadWriteLock();
    mReadLock = mLock.readLock();
    mWriteLock = mLock.writeLock();
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    for (Map.Entry<String, MountInfo> entry : mMountTable.entrySet()) {
      String alluxioPath = entry.getKey();
      MountInfo info = entry.getValue();

      // do not journal the root mount point
      if (alluxioPath.equals(ROOT)) {
        continue;
      }

      Map<String, String> properties = info.getOptions().getProperties();
      List<File.StringPairEntry> protoProperties = new ArrayList<>(properties.size());
      for (Map.Entry<String, String> property : properties.entrySet()) {
        protoProperties.add(File.StringPairEntry.newBuilder()
            .setKey(property.getKey())
            .setValue(property.getValue())
            .build());
      }

      AddMountPointEntry addMountPoint = AddMountPointEntry.newBuilder().setAlluxioPath(alluxioPath)
          .setUfsPath(info.getUfsUri().toString()).setReadOnly(info.getOptions().isReadOnly())
          .addAllProperties(protoProperties).setShared(info.getOptions().isShared()).build();
      Journal.JournalEntry journalEntry =
          Journal.JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build();
      outputStream.write(journalEntry);
    }
  }

  /**
   * Mounts the given UFS path at the given Alluxio path. The Alluxio path should not be nested
   * under an existing mount point.
   *
   * @param alluxioUri an Alluxio path URI
   * @param ufsUri a UFS path URI
   * @param options the mount options
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public void add(AlluxioURI alluxioUri, AlluxioURI ufsUri, MountOptions options)
      throws FileAlreadyExistsException, InvalidPathException {
    String alluxioPath = alluxioUri.getPath();
    LOG.info("Mounting {} at {}", ufsUri, alluxioPath);

    try (LockResource r = new LockResource(mWriteLock)) {
      if (mMountTable.containsKey(alluxioPath)) {
        throw new FileAlreadyExistsException(
            ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(alluxioPath));
      }
      // Check all non-root mount points, to check if they're a prefix of the alluxioPath we're
      // trying to mount. Also make sure that the ufs path we're trying to mount is not a prefix
      // or suffix of any existing mount path.
      for (Map.Entry<String, MountInfo> entry : mMountTable.entrySet()) {
        String mountedAlluxioPath = entry.getKey();
        AlluxioURI mountedUfsUri = entry.getValue().getUfsUri();
        if (!mountedAlluxioPath.equals(ROOT)
            && PathUtils.hasPrefix(alluxioPath, mountedAlluxioPath)) {
          throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage(
              mountedAlluxioPath, alluxioPath));
        }
        if ((ufsUri.getScheme() == null || ufsUri.getScheme().equals(mountedUfsUri.getScheme()))
            && (ufsUri.getAuthority() == null || ufsUri.getAuthority()
            .equals(mountedUfsUri.getAuthority()))) {
          String ufsPath = ufsUri.getPath();
          String mountedUfsPath = mountedUfsUri.getPath();
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
      mMountTable.put(alluxioPath, new MountInfo(ufsUri, options));
    }
  }

  /**
   * Unmounts the given Alluxio path. The path should match an existing mount point.
   *
   * @param uri an Alluxio path URI
   * @return whether the operation succeeded or not
   */
  public boolean delete(AlluxioURI uri) {
    String path = uri.getPath();
    LOG.info("Unmounting {}", path);
    if (path.equals(ROOT)) {
      LOG.warn("Cannot unmount the root mount point.");
      return false;
    }

    try (LockResource r = new LockResource(mWriteLock)) {
      if (mMountTable.containsKey(path)) {
        mMountTable.remove(path);
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
    String mountPoint = null;

    try (LockResource r = new LockResource(mReadLock)) {
      for (Map.Entry<String, MountInfo> entry : mMountTable.entrySet()) {
        String alluxioPath = entry.getKey();
        if (PathUtils.hasPrefix(path, alluxioPath)
            && (mountPoint == null || PathUtils.hasPrefix(alluxioPath, mountPoint))) {
          mountPoint = alluxioPath;
        }
      }
      return mountPoint;
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
      return new HashMap<>(mMountTable);
    }
  }

  /**
   * @param uri an Alluxio path URI
   * @return whether the given uri is a mount point
   */
  public boolean isMountPoint(AlluxioURI uri) {
    try (LockResource r = new LockResource(mReadLock)) {
      return mMountTable.containsKey(uri.getPath());
    }
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
      // This will re-acquire the read lock, but that is allowed.
      String mountPoint = getMountPoint(uri);
      if (mountPoint != null) {
        MountInfo info = mMountTable.get(mountPoint);
        AlluxioURI ufsUri = info.getUfsUri();
        // TODO(gpang): this ufs should probably be cached.
        UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsUri.toString());
        ufs.setProperties(info.getOptions().getProperties());
        AlluxioURI resolvedUri = ufs.resolveUri(ufsUri, path.substring(mountPoint.length()));
        return new Resolution(resolvedUri, ufs, info.getOptions().isShared());
      }
      return new Resolution(uri, null, false);
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
      MountInfo mountInfo = mMountTable.get(mountPoint);
      if (mountInfo.getOptions().isReadOnly()) {
        throw new AccessControlException(ExceptionMessage.MOUNT_READONLY, alluxioUri, mountPoint);
      }
    }
  }

  /**
   * This class represents a UFS path after resolution. The UFS URI and the {@link UnderFileSystem}
   * for the UFS path are available.
   */
  public final class Resolution {
    private final AlluxioURI mUri;
    private final UnderFileSystem mUfs;
    private final boolean mShared;

    private Resolution(AlluxioURI uri, UnderFileSystem ufs, boolean shared) {
      mUri = uri;
      mUfs = ufs;
      mShared = shared;
    }

    /**
     * @return the URI in the ufs
     */
    public AlluxioURI getUri() {
      return mUri;
    }

    /**
     * @return the {@link UnderFileSystem} instance
     */
    public UnderFileSystem getUfs() {
      return mUfs;
    }

    /**
     * @return the shared option
     */
    public boolean getShared() {
      return mShared;
    }
  }
}
