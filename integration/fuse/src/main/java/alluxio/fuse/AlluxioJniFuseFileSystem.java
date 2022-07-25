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

package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.cli.FuseShell;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.auth.AuthPolicyFactory;
import alluxio.fuse.file.FuseFileEntry;
import alluxio.fuse.file.FuseFileStream;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.FuseException;
import alluxio.jnifuse.FuseFillDir;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.LogUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.BlockMasterInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main FUSE implementation class.
 * <p>
 * Implements the FUSE callbacks defined by jni-fuse.
 */
@ThreadSafe
public final class AlluxioJniFuseFileSystem extends AbstractFuseFileSystem
    implements FuseUmountable {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJniFuseFileSystem.class);

  private final FileSystem mFileSystem;
  private final FileSystemContext mFileSystemContext;
  // Caches the filesystem statistics for Fuse.statfs
  private final Supplier<BlockMasterInfo> mFsStatCache;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;
  // Cache Uid<->Username and Gid<->Groupname mapping for local OS
  private final LoadingCache<String, Long> mUidCache;
  private final LoadingCache<String, Long> mGidCache;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final FuseShell mFuseShell;
  private final AlluxioFuseFileSystemOpts mFuseFsOpts;
  private static final IndexDefinition<FuseFileEntry<FuseFileStream>, Long>
      ID_INDEX =
      new IndexDefinition<FuseFileEntry<FuseFileStream>, Long>(true) {
        @Override
        public Long getFieldValue(FuseFileEntry<FuseFileStream> o) {
          return o.getId();
        }
      };

  // Add a PATH_INDEX to know getattr() been called when writing this file
  private static final IndexDefinition<FuseFileEntry<FuseFileStream>, String>
      PATH_INDEX =
      new IndexDefinition<FuseFileEntry<FuseFileStream>, String>(true) {
        @Override
        public String getFieldValue(FuseFileEntry<FuseFileStream> o) {
          return o.getPath();
        }
      };
  private final IndexedSet<FuseFileEntry<FuseFileStream>> mFileEntries
      = new IndexedSet<>(ID_INDEX, PATH_INDEX);
  private final AuthPolicy mAuthPolicy;
  private final FuseFileStream.Factory mStreamFactory;

  /** df command will treat -1 as an unknown value. */
  @VisibleForTesting
  public static final int UNKNOWN_INODES = -1;

  /**
   * Creates a new instance of {@link AlluxioJniFuseFileSystem}.
   *
   * @param fsContext the file system context
   * @param fs Alluxio file system
   * @param fuseFsOpts options for fuse filesystem
   */
  public AlluxioJniFuseFileSystem(
      FileSystemContext fsContext, FileSystem fs, AlluxioFuseFileSystemOpts fuseFsOpts) {
    super(Paths.get(fuseFsOpts.getMountPoint()));
    mFileSystemContext = fsContext;
    mFileSystem = fs;
    mFuseFsOpts = fuseFsOpts;
    mFuseShell = new FuseShell(fs, fuseFsOpts);
    long statCacheTimeout = fuseFsOpts.getStatCacheTimeout();
    mFsStatCache = statCacheTimeout > 0 ? Suppliers.memoizeWithExpiration(
        this::acquireBlockMasterInfo, statCacheTimeout, TimeUnit.MILLISECONDS)
        : this::acquireBlockMasterInfo;
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(fuseFsOpts.getFuseMaxPathCached())
        .build(new CacheLoader<String, AlluxioURI>() {
          @Override
          public AlluxioURI load(String fusePath) {
            // fusePath is guaranteed to always be an absolute path (i.e., starts
            // with a fwd slash) - relative to the FUSE mount point
            final String relPath = fusePath.substring(1);
            final Path tpath = Paths.get(fuseFsOpts.getAlluxioPath()).resolve(relPath);
            return new AlluxioURI(tpath.toString());
          }
        });
    mUidCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<String, Long>() {
          @Override
          public Long load(String userName) {
            return AlluxioFuseUtils.getUid(userName);
          }
        });
    mGidCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<String, Long>() {
          @Override
          public Long load(String groupName) {
            return AlluxioFuseUtils.getGidFromGroupName(groupName);
          }
        });
    mAuthPolicy = AuthPolicyFactory.create(mFileSystem, fuseFsOpts, this);
    mStreamFactory = new FuseFileStream.Factory(mFileSystem, mAuthPolicy);
    if (fuseFsOpts.isDebug()) {
      try {
        LogUtils.setLogLevel(this.getClass().getName(), org.slf4j.event.Level.DEBUG.toString());
      } catch (IOException e) {
        LOG.error("Failed to set AlluxioJniFuseFileSystem log to debug level", e);
      }
    }
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.FUSE_READ_WRITE_FILE_COUNT.getName()),
        mFileEntries::size);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.FUSE_CACHED_PATH_COUNT.getName()),
        mPathResolverCache::size);
  }

  @Override
  public int create(String path, long mode, FuseFileInfo fi) {
    int originalFlags = fi.flags.get();
    fi.flags.set(OpenFlags.O_WRONLY.intValue());
    return AlluxioFuseUtils.call(LOG, () -> createOrOpenInternal(path, fi, mode),
        "Fuse.Create", "path=%s,mode=%o,flags=0x%x", path, mode, originalFlags);
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG,
        () -> createOrOpenInternal(path, fi, AlluxioFuseUtils.MODE_NOT_SET_VALUE),
        "Fuse.Open", "path=%s,flags=0x%x", path, fi.flags.get());
  }

  private int createOrOpenInternal(String path, FuseFileInfo fi, long mode) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    FuseFileStream stream = mStreamFactory.create(uri, fi.flags.get(), mode);
    long fd = mNextOpenFileId.getAndIncrement();
    mFileEntries.add(new FuseFileEntry<>(fd, path, stream));
    fi.fh.set(fd);
    return 0;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    return AlluxioFuseUtils.call(
        LOG, () -> getattrInternal(path, stat), "Fuse.Getattr", "path=%s", path);
  }

  private int getattrInternal(String path, FileStat stat) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    try {
      URIStatus status;
      // Handle special metadata cache operation
      if (mFuseFsOpts.isSpecialCommandEnabled() && mFuseShell.isSpecialCommand(uri)) {
        // TODO(lu) add cache for isFuseSpecialCommand if needed
        status = mFuseShell.runCommand(uri);
      } else {
        status = mFileSystem.getStatus(uri);
      }
      long size = status.getLength();
      if (!status.isCompleted()) {
        FuseFileEntry<FuseFileStream> stream = mFileEntries.getFirstByField(PATH_INDEX, path);
        if (stream != null) {
          size = stream.getFileStream().getFileLength();
        } else if (!AlluxioFuseUtils.waitForFileCompleted(mFileSystem, uri)) {
          // Always block waiting for file to be completed except when the file is writing
          // We do not want to block the writing process
          LOG.error("File {} is not completed", path);
        } else {
          // Update the file status after waiting
          status = mFileSystem.getStatus(uri);
          size = status.getLength();
        }
      }
      stat.st_size.set(size);

      // Sets block number to fulfill du command needs
      // `st_blksize` is ignored in `getattr` according to
      // https://github.com/libfuse/libfuse/blob/d4a7ba44b022e3b63fc215374d87ed9e930d9974/include/fuse.h#L302
      // According to http://man7.org/linux/man-pages/man2/stat.2.html,
      // `st_blocks` is the number of 512B blocks allocated
      stat.st_blocks.set((int) Math.ceil((double) size / 512));

      final long ctime_sec = status.getLastModificationTimeMs() / 1000;
      final long atime_sec = status.getLastAccessTimeMs() / 1000;
      // Keeps only the "residual" nanoseconds not caputred in citme_sec
      final long ctime_nsec = (status.getLastModificationTimeMs() % 1000) * 1_000_000L;
      final long atime_nsec = (status.getLastAccessTimeMs() % 1000) * 1_000_000L;

      stat.st_atim.tv_sec.set(atime_sec);
      stat.st_atim.tv_nsec.set(atime_nsec);
      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);

      if (mFuseFsOpts.isUserGroupTranslationEnabled()) {
        // Translate the file owner/group to unix uid/gid
        // Show as uid==-1 (nobody) if owner does not exist in unix
        // Show as gid==-1 (nogroup) if group does not exist in unix
        stat.st_uid.set(mUidCache.get(status.getOwner()));
        stat.st_gid.set(mGidCache.get(status.getGroup()));
      } else {
        stat.st_uid.set(AlluxioFuseUtils.DEFAULT_UID);
        stat.st_gid.set(AlluxioFuseUtils.DEFAULT_GID);
      }

      int mode = status.getMode();
      if (status.isFolder()) {
        mode |= FileStat.S_IFDIR;
      } else {
        mode |= FileStat.S_IFREG;
      }
      stat.st_mode.set(mode);
      stat.st_nlink.set(1);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to getattr {}: path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (AccessControlException e) {
      LOG.error("Failed to getattr {}: permission denied", path, e);
      return -ErrorCodes.EACCES();
    } catch (Throwable t) {
      LOG.error("Failed to getattr {}", path, t);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int readdir(String path, long buff, long filter, long offset,
      FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readdirInternal(path, buff, filter, offset, fi),
        "Fuse.Readdir", "path=%s", path);
  }

  private int readdirInternal(String path, long buff, long filter, long offset,
      FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    try {
      // standard . and .. entries
      FuseFillDir.apply(filter, buff, ".", null, 0);
      FuseFillDir.apply(filter, buff, "..", null, 0);

      mFileSystem.iterateStatus(uri, file -> {
        FuseFillDir.apply(filter, buff, file.getName(), null, 0);
      });
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to readdir {}", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    final long fd = fi.fh.get();
    return AlluxioFuseUtils.call(LOG, () -> readInternal(path, buf, size, offset, fd),
        "Fuse.Read", "path=%s,fd=%d,size=%d,offset=%d",
        path, fd, size, offset);
  }

  private int readInternal(
      String path, ByteBuffer buf, long size, long offset, long fd) {
    FuseFileEntry<FuseFileStream> entry = mFileEntries.getFirstByField(ID_INDEX, fd);
    if (entry == null) {
      LOG.error("Failed to read {}: Cannot find fd {}", path, fd);
      return -ErrorCodes.EBADFD();
    }
    return entry.getFileStream().read(buf, size, offset);
  }

  @Override
  public int write(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    final long fd = fi.fh.get();
    return AlluxioFuseUtils.call(LOG, () -> writeInternal(path, buf, size, offset, fd),
        "Fuse.Write", "path=%s,fd=%d,size=%d,offset=%d",
        path, fd, size, offset);
  }

  private int writeInternal(
      String path, ByteBuffer buf, long size, long offset, long fd) {
    FuseFileEntry<FuseFileStream> entry = mFileEntries.getFirstByField(ID_INDEX, fd);
    if (entry == null) {
      LOG.error("Failed to write {}: Cannot find fd {}", path, fd);
      return -ErrorCodes.EBADFD();
    }
    entry.getFileStream().write(buf, size, offset);
    return (int) size;
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    final long fd = fi.fh.get();
    return AlluxioFuseUtils.call(LOG, () -> flushInternal(path, fd), "Fuse.Flush", "path=%s,fd=%s",
        path, fd);
  }

  private int flushInternal(String path, long fd) {
    FuseFileEntry<FuseFileStream> entry = mFileEntries.getFirstByField(ID_INDEX, fd);
    if (entry == null) {
      LOG.error("Failed to flush {}: Cannot find fd {}", path, fd);
      return -ErrorCodes.EBADFD();
    }
    entry.getFileStream().flush();
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    long fd = fi.fh.get();
    return AlluxioFuseUtils.call(LOG, () -> releaseInternal(path, fd),
        "Fuse.Release", "path=%s,fd=%s", path, fd);
  }

  private int releaseInternal(String path, long fd) {
    FuseFileEntry<FuseFileStream> entry = mFileEntries.getFirstByField(ID_INDEX, fd);
    if (entry == null) {
      LOG.error("Failed to release {}: Cannot find fd {}", path, fd);
      return -ErrorCodes.EBADFD();
    }
    try {
      entry.getFileStream().close();
    } finally {
      mFileEntries.remove(entry);
    }
    return 0;
  }

  @Override
  public int mkdir(String path, long mode) {
    return AlluxioFuseUtils.call(LOG, () -> mkdirInternal(path, mode),
        "Fuse.Mkdir", "path=%s,mode=%o,", path, mode);
  }

  private int mkdirInternal(String path, long mode) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    try {
      mFileSystem.createDirectory(uri,
          CreateDirectoryPOptions.newBuilder()
              .setMode(new Mode((short) mode).toProto())
              .build());
      mAuthPolicy.setUserGroupIfNeeded(uri);
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to mkdir {}", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int unlink(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "Fuse.Unlink", "path=%s", path);
  }

  @Override
  public int rmdir(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "Fuse.Rmdir", "path=%s", path);
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    try {
      mFileSystem.delete(uri);
    } catch (DirectoryNotEmptyException de) {
      LOG.error("Failed to remove {}: directory not empty", path, de);
      return -ErrorCodes.EEXIST() | ErrorCodes.ENOTEMPTY();
    } catch (FileDoesNotExistException fe) {
      LOG.error("Failed to remove {}: path does not exist", path, fe);
      return -ErrorCodes.ENOENT();
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to remove {}: ", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int rename(String oldPath, String newPath, int flags) {
    return AlluxioFuseUtils.call(LOG, () -> renameInternal(oldPath, newPath, flags),
        "Fuse.Rename", "oldPath=%s,newPath=%s,", oldPath, newPath);
  }

  private int renameInternal(String sourcePath, String destPath, int flags) {
    final AlluxioURI sourceUri = mPathResolverCache.getUnchecked(sourcePath);
    final AlluxioURI destUri = mPathResolverCache.getUnchecked(destPath);
    int res = AlluxioFuseUtils.checkFileLength(destUri);
    if (res != 0) {
      return res;
    }
    Optional<URIStatus> sourceStatus = AlluxioFuseUtils.getPathStatus(mFileSystem, sourceUri);
    if (!sourceStatus.isPresent()) {
      LOG.error("Failed to rename {} to {}: source non-existing", sourcePath, destPath);
      return -ErrorCodes.EEXIST();
    }
    if (!sourceStatus.get().isCompleted()) {
      // TODO(lu) https://github.com/Alluxio/alluxio/issues/14854
      // how to support rename while writing
      LOG.error("Failed to rename {} to {}: source is incomplete", sourcePath, destPath);
      return -ErrorCodes.EIO();
    }
    Optional<URIStatus> destStatus = AlluxioFuseUtils.getPathStatus(mFileSystem, destUri);
    try {
      if (destStatus.isPresent()) {
        if (AlluxioJniRenameUtils.exchange(flags)) {
          LOG.error("Failed to rename {} to {}, not support RENAME_EXCHANGE flags",
              sourcePath, destPath);
          return -ErrorCodes.ENOTSUP();
        }
        if (AlluxioJniRenameUtils.noreplace(flags)) {
          LOG.error("Failed to rename {} to {}, overwriting destination with RENAME_NOREPLACE flag",
              sourcePath, destPath);
          return -ErrorCodes.EEXIST();
        } else if (AlluxioJniRenameUtils.noFlags(flags)) {
          try {
            mFileSystem.delete(destUri);
          } catch (DirectoryNotEmptyException e) {
            return -ErrorCodes.ENOTEMPTY();
          }
        } else {
          LOG.error("Failed to rename {} to {}, unknown flags {}",
              sourcePath, destPath, flags);
          return -ErrorCodes.EINVAL();
        }
      } else if (AlluxioJniRenameUtils.exchange(flags)) {
        LOG.error("Failed to rename {} to {}, destination file/dir must exist to RENAME_EXCHANGE",
            sourcePath, destPath);
      }
      mFileSystem.rename(sourceUri, destUri);
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to rename {} to {}", sourcePath, destPath, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int chmod(String path, long mode) {
    return AlluxioFuseUtils.call(LOG, () -> chmodInternal(path, mode),
        "Fuse.Chmod", "path=%s,mode=%o", path, mode);
  }

  private int chmodInternal(String path, long mode) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    AlluxioFuseUtils.setAttribute(mFileSystem, mPathResolverCache.getUnchecked(path),
        SetAttributePOptions.newBuilder()
            .setMode(new Mode((short) mode).toProto()).build());
    return 0;
  }

  @Override
  public int chown(String path, long uid, long gid) {
    return AlluxioFuseUtils.call(LOG, () -> chownInternal(path, uid, gid),
        "Fuse.Chown", "path=%s,uid=%d,gid=%d", path, uid, gid);
  }

  private int chownInternal(String path, long uid, long gid) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    if (!mFuseFsOpts.isUserGroupTranslationEnabled()) {
      LOG.warn("Failed to chown {}: "
          + "Please set {} to true to enable user group translation in Alluxio-FUSE.",
          path, PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
      return -ErrorCodes.EOPNOTSUPP();
    }

    SetAttributePOptions.Builder optionsBuilder = SetAttributePOptions.newBuilder();
    String userName = "";
    if (uid != AlluxioFuseUtils.ID_NOT_SET_VALUE
        && uid != AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED) {
      userName = AlluxioFuseUtils.getUserName(uid);
      if (userName.isEmpty()) {
        // This should never be reached
        LOG.error("Failed to chown {}: failed to get user name from uid {}", path, uid);
        return -ErrorCodes.EINVAL();
      }
      optionsBuilder.setOwner(userName);
    }

    String groupName;
    if (gid != AlluxioFuseUtils.ID_NOT_SET_VALUE
        && gid != AlluxioFuseUtils.ID_NOT_SET_VALUE_UNSIGNED) {
      groupName = AlluxioFuseUtils.getGroupName(gid);
      if (groupName.isEmpty()) {
        // This should never be reached
        LOG.error("Failed to chown {}: failed to get group name from gid {}", path, gid);
        return -ErrorCodes.EINVAL();
      }
      optionsBuilder.setGroup(groupName);
    } else if (!userName.isEmpty()) {
      groupName = AlluxioFuseUtils.getGroupName(userName);
      optionsBuilder.setGroup(groupName);
    }
    AlluxioFuseUtils.setAttribute(mFileSystem, mPathResolverCache.getUnchecked(path),
        optionsBuilder.build());
    return 0;
  }

  /**
   * Since files can be written only once, only sequentially,
   * and never be modified in Alluxio, truncate is not supported internally by Alluxio.
   *
   * In Alluxio Fuse, we support truncate in some special cases.
   *
   * @param path the file to truncate
   * @param size the size to truncate to
   * @return 0 if succeed, error code otherwise
   */
  @Override
  public int truncate(String path, long size) {
    return AlluxioFuseUtils.call(LOG, () -> truncateInternal(path, size),
        "Fuse.Truncate", "path=%s,size=%d", path, size);
  }

  private int truncateInternal(String path, long size) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    FuseFileEntry<FuseFileStream> entry = mFileEntries.getFirstByField(PATH_INDEX, path);
    if (entry != null) {
      entry.getFileStream().truncate(size);
      return 0;
    }
    Optional<URIStatus> status = AlluxioFuseUtils.getPathStatus(mFileSystem, uri);
    if (!status.isPresent()) {
      if (size == 0) {
        return 0;
      }
      LOG.error("Failed to truncate file {} to {} bytes: file does not exist", path, size);
      return -ErrorCodes.EEXIST();
    }

    if (status.get().isCompleted()) {
      long fileLen = status.get().getLength();
      if (fileLen == size) {
        return 0;
      }
      if (size == 0) {
        AlluxioFuseUtils.deletePath(mFileSystem, uri);
      }
      LOG.error("Failed to truncate file {}({} bytes) to {} bytes: not supported.",
          path, fileLen, size);
      return -ErrorCodes.EOPNOTSUPP();
    }

    LOG.error("Failed to truncate file {} to {} bytes: "
        + "file is being written by other Fuse applications or Alluxio APIs.",
        path, size);
    return -ErrorCodes.EOPNOTSUPP();
  }

  @Override
  public int utimens(String path, long aSec, long aNsec, long mSec, long mNsec) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    // TODO(maobaolong): implements this logic for alluxio.
    LOG.debug("utimens for {}, but do nothing for this filesystem", path);
    return 0;
  }

  @Override
  public int symlink(String linkname, String path) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    LOG.warn("Not supported symlink operation, linkname {}, path{}", linkname, path);
    return -ErrorCodes.ENOTSUP();
  }

  /**
   * Gets the filesystem statistics.
   *
   * @param path The FS path of the directory
   * @param stbuf Statistics of a filesystem
   * @return 0 on success, a negative value on error
   */
  @Override
  public int statfs(String path, Statvfs stbuf) {
    return AlluxioFuseUtils.call(LOG, () -> statfsInternal(path, stbuf),
        "Fuse.Statfs", "path=%s", path);
  }

  private int statfsInternal(String path, Statvfs stbuf) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkFileLength(uri);
    if (res != 0) {
      return res;
    }
    BlockMasterInfo info = mFsStatCache.get();
    if (info == null) {
      LOG.error("Failed to statfs {}: cannot get block master info", path);
      return -ErrorCodes.EIO();
    }
    long blockSize = 16L * Constants.KB;
    // fs block size
    // The size in bytes of the minimum unit of allocation on this file system
    stbuf.f_bsize.set(blockSize);
    // The preferred length of I/O requests for files on this file system.
    stbuf.f_frsize.set(blockSize);
    // total data blocks in fs
    stbuf.f_blocks.set(info.getCapacityBytes() / blockSize);
    // free blocks in fs
    long freeBlocks = info.getFreeBytes() / blockSize;
    stbuf.f_bfree.set(freeBlocks);
    stbuf.f_bavail.set(freeBlocks);
    // inode info in fs
    stbuf.f_files.set(UNKNOWN_INODES);
    stbuf.f_ffree.set(UNKNOWN_INODES);
    stbuf.f_favail.set(UNKNOWN_INODES);
    // max file name length
    stbuf.f_namemax.set(AlluxioFuseUtils.MAX_NAME_LENGTH);
    return 0;
  }

  @Nullable
  private BlockMasterInfo acquireBlockMasterInfo() {
    try (CloseableResource<BlockMasterClient> masterClientResource =
             mFileSystemContext.acquireBlockMasterClientResource()) {
      Set<BlockMasterInfo.BlockMasterInfoField> blockMasterInfoFilter =
          new HashSet<>(Arrays.asList(
              BlockMasterInfo.BlockMasterInfoField.CAPACITY_BYTES,
              BlockMasterInfo.BlockMasterInfoField.FREE_BYTES,
              BlockMasterInfo.BlockMasterInfoField.USED_BYTES));
      return masterClientResource.get().getBlockMasterInfo(blockMasterInfoFilter);
    } catch (IOException e) {
      LOG.error("Failed to acquire block master information", e);
      return null;
    }
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFileSystemName() {
    return mFuseFsOpts.getFsName();
  }

  @Override
  public void umount(boolean force) throws FuseException {
    // Release operation is async, we will try our best efforts to
    // close all opened file in/out stream before umounting the fuse
    if (mFuseFsOpts.getFuseUmountTimeout() > 0 && (!mFileEntries.isEmpty())) {
      LOG.info("Unmounting {}. Waiting for all in progress file read/write to finish",
          mFuseFsOpts.getMountPoint());
      try {
        CommonUtils.waitFor("all in progress file read/write to finish",
            mFileEntries::isEmpty,
            WaitForOptions.defaults().setTimeoutMs(mFuseFsOpts.getFuseUmountTimeout()));
      } catch (InterruptedException e) {
        LOG.error("Unmount {} interrupted", mFuseFsOpts.getMountPoint());
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        LOG.error("Timeout when waiting all in progress file read/write to finish "
            + "when unmounting {}. {} fileInStream remain unclosed. "
            + "{} fileOutStream remain unclosed.",
            mFuseFsOpts.getMountPoint(), mFileEntries.size());
        if (!force) {
          throw new FuseException("Timed out for umount due to device is busy.");
        }
      }
    }
    super.umount(force);
  }

  @VisibleForTesting
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }
}
