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
import alluxio.client.file.options.FileSystemOptions;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.AlreadyExistsRuntimeException;
import alluxio.exception.runtime.CancelledRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.auth.AuthPolicyFactory;
import alluxio.fuse.file.FuseFileEntry;
import alluxio.fuse.file.FuseFileStream;
import alluxio.fuse.options.FuseOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.ErrorType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
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
import com.google.common.cache.LoadingCache;
import io.grpc.Status;
import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
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

  private final AlluxioConfiguration mConf;
  private final FileSystem mFileSystem;
  private final FileSystemContext mFileSystemContext;
  // Caches the filesystem statistics for Fuse.statfs
  private final Supplier<BlockMasterInfo> mFsStatCache;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final FuseShell mFuseShell;
  private static final IndexDefinition<FuseFileEntry<FuseFileStream>, Long>
      ID_INDEX = IndexDefinition.ofUnique(FuseFileEntry::getId);

  // Add a PATH_INDEX to know getattr() been called when writing this file
  private static final IndexDefinition<FuseFileEntry<FuseFileStream>, String>
      PATH_INDEX = IndexDefinition.ofUnique(FuseFileEntry::getPath);
  private final IndexedSet<FuseFileEntry<FuseFileStream>> mFileEntries
      = new IndexedSet<>(ID_INDEX, PATH_INDEX);
  private final AuthPolicy mAuthPolicy;
  private final FuseFileStream.Factory mStreamFactory;
  private final boolean mUfsEnabled;

  /** df command will treat -1 as an unknown value. */
  @VisibleForTesting
  public static final int UNKNOWN_INODES = -1;

  /**
   * Creates a new instance of {@link AlluxioJniFuseFileSystem}.
   *
   * @param fsContext the file system context
   * @param fs Alluxio file system
   * @param fuseOptions the fuse options
   */
  public AlluxioJniFuseFileSystem(FileSystemContext fsContext, FileSystem fs,
      FuseOptions fuseOptions) {
    super(Paths.get(fsContext.getClusterConf().getString(PropertyKey.FUSE_MOUNT_POINT)));
    mFileSystemContext = fsContext;
    mFileSystem = fs;
    mConf = fsContext.getClusterConf();
    mFuseShell = new FuseShell(fs, mConf);
    long statCacheTimeout = mConf.getMs(PropertyKey.FUSE_STAT_CACHE_REFRESH_INTERVAL);
    mFsStatCache = statCacheTimeout > 0 ? Suppliers.memoizeWithExpiration(
        this::acquireBlockMasterInfo, statCacheTimeout, TimeUnit.MILLISECONDS)
        : this::acquireBlockMasterInfo;
    mPathResolverCache = AlluxioFuseUtils.getPathResolverCache(mConf, fuseOptions);
    mAuthPolicy = AuthPolicyFactory.create(mFileSystem, mConf, this);
    mStreamFactory = new FuseFileStream.Factory(mFileSystem, mAuthPolicy);
    mUfsEnabled = fuseOptions.getFileSystemOptions().getFileSystemType()
        == FileSystemOptions.FileSystemType.Ufs;
    if (mConf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED)) {
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
    if (res != 0) {
      return res;
    }
    try {
      FuseFileStream stream = mStreamFactory.create(uri, fi.flags.get(), mode);
      long fd = mNextOpenFileId.getAndIncrement();
      mFileEntries.add(new FuseFileEntry<>(fd, path, stream));
      fi.fh.set(fd);
    } catch (NotFoundRuntimeException e) {
      LOG.error("Failed to read {}: path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (AlreadyExistsRuntimeException e) {
      LOG.error("Failed to write {}: path already exist", path);
      return -ErrorCodes.EEXIST();
    }
    return 0;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    return AlluxioFuseUtils.call(
        LOG, () -> getattrInternal(path, stat), "Fuse.Getattr", "path=%s", path);
  }

  private int getattrInternal(String path, FileStat stat) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkNameLength(uri);
    if (res != 0) {
      return res;
    }
    try {
      URIStatus status;
      // Handle special metadata cache operation
      if (mConf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED)
          && mFuseShell.isSpecialCommand(uri)) {
        // TODO(lu) add cache for isFuseSpecialCommand if needed
        status = mFuseShell.runCommand(uri);
      } else {
        try {
          status = mFileSystem.getStatus(uri);
        } catch (FileNotFoundException e) {
          // TODO(lu) reconsider the logic
          FuseFileEntry<FuseFileStream> stream = mFileEntries.getFirstByField(PATH_INDEX, path);
          if (stream != null) {
            long size = stream.getFileStream().getFileLength();
            stat.st_size.set(size);
            stat.st_blocks.set((int) Math.ceil((double) size / 512));
            // TODO(lu) create URI status when creating the file?
            return 0;
          } else {
            throw e;
          }
        }
      }
      long size = status.getLength();
      if (!status.isCompleted()) {
        FuseFileEntry<FuseFileStream> stream = mFileEntries.getFirstByField(PATH_INDEX, path);
        if (stream != null) {
          size = stream.getFileStream().getFileLength();
        } else {
          Optional<URIStatus> optionalStatus
              = AlluxioFuseUtils.waitForFileCompleted(mFileSystem, uri);
          if (optionalStatus.isPresent()) {
            status = optionalStatus.get();
            size = status.getLength();
          } else {
            LOG.error("File {} is not completed", path);
          }
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

      stat.st_uid.set(mAuthPolicy.getUid(status.getOwner())
          .orElse(AlluxioFuseUtils.ID_NOT_SET_VALUE));
      stat.st_gid.set(mAuthPolicy.getGid(status.getGroup())
          .orElse(AlluxioFuseUtils.ID_NOT_SET_VALUE));

      int mode = status.getMode();
      if (status.isFolder()) {
        mode |= FileStat.S_IFDIR;
      } else {
        mode |= FileStat.S_IFREG;
      }
      stat.st_mode.set(mode);
      stat.st_nlink.set(1);
    } catch (FileDoesNotExistException | InvalidPathException | FileNotFoundException e) {
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    try {
      return entry.getFileStream().read(buf, size, offset);
    } catch (NotFoundRuntimeException e) {
      LOG.error("Failed to read {}: File does not exist or is writing by other clients", path);
      return -ErrorCodes.ENOENT();
    }
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
    try {
      entry.getFileStream().write(buf, size, offset);
    } catch (AlreadyExistsRuntimeException e) {
      LOG.error("Failed to write {}: cannot overwrite existing file", path);
      return -ErrorCodes.EEXIST();
    }
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    int res = AlluxioFuseUtils.checkNameLength(destUri);
    if (res != 0) {
      return res;
    }
    Optional<URIStatus> sourceStatus = AlluxioFuseUtils.getPathStatus(mFileSystem, sourceUri);
    if (!sourceStatus.isPresent()) {
      LOG.error("Failed to rename {} to {}: source non-existing", sourcePath, destPath);
      return -ErrorCodes.ENOENT();
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
    if (res != 0) {
      return res;
    }
    mAuthPolicy.setUserGroup(uri, uid, gid);
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    if (mUfsEnabled) {
      return 0;
    }
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int res = AlluxioFuseUtils.checkNameLength(uri);
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
    return mConf.getString(PropertyKey.FUSE_FS_NAME);
  }

  @Override
  public void umount(boolean force) {
    // Release operation is async, we will try our best efforts to
    // close all opened file in/out stream before umounting the fuse
    long unmountTimeout = mConf.getMs(PropertyKey.FUSE_UMOUNT_TIMEOUT);
    String mountpoint = mConf.getString(PropertyKey.FUSE_MOUNT_POINT);
    if (unmountTimeout > 0 && (!mFileEntries.isEmpty())) {
      LOG.info("Unmounting {}. Waiting for all in progress file read/write to finish",
          mountpoint);
      try {
        CommonUtils.waitFor("all in progress file read/write to finish",
            mFileEntries::isEmpty,
            WaitForOptions.defaults().setTimeoutMs((int) unmountTimeout));
      } catch (InterruptedException e) {
        LOG.error("Unmount {} interrupted", mountpoint);
        Thread.currentThread().interrupt();
        throw new CancelledRuntimeException("Unmount interrupted", e);
      } catch (TimeoutException e) {
        LOG.error("Timeout when waiting all in progress file read/write to finish "
            + "when unmounting {}. {} file streams remain unclosed.",
            mountpoint, mFileEntries.size());
        if (!force) {
          throw new AlluxioRuntimeException(Status.DEADLINE_EXCEEDED,
              "Timed out for umount due to device is busy.", e, ErrorType.External, false);
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
