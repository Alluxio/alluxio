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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.FuseFillDir;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseContext;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;
import alluxio.security.authorization.Mode;
import alluxio.util.ThreadUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main FUSE implementation class.
 * <p>
 * Implements the FUSE callbacks defined by jni-fuse.
 */
@ThreadSafe
public final class AlluxioJniFuseFileSystem extends AbstractFuseFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJniFuseFileSystem.class);
  private final FileSystem mFileSystem;
  private final AlluxioConfiguration mConf;
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final AtomicLong mOpenOps = new AtomicLong(0);
  private final AtomicLong mReleaseOps = new AtomicLong(0);
  private final AtomicLong mReadOps = new AtomicLong(0);
  private final String mFsName;

  private static final int LOCK_SIZE = 20480;
  /** A readwrite lock pool to guard individual files based on striping. */
  private final ReadWriteLock[] mFileLocks = new ReentrantReadWriteLock[LOCK_SIZE];

  private final Map<Long, FileInStream> mOpenFileEntries = new ConcurrentHashMap<>();
  private final Map<Long, FileOutStream> mCreateFileEntries = new ConcurrentHashMap<>();
  private final boolean mIsUserGroupTranslation;

  // To make test build
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE = -1;
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;
  /**
   * df command will treat -1 as an unknown value.
   */
  @VisibleForTesting
  public static final int UNKNOWN_INODES = -1;
  /**
   * Most FileSystems on linux limit the length of file name beyond 255 characters.
   */
  @VisibleForTesting
  public static final int MAX_NAME_LENGTH = 255;

  private static final String USER_NAME = System.getProperty("user.name");
  private static final String GROUP_NAME = System.getProperty("user.name");
  private static final long UID = AlluxioFuseUtils.getUid(USER_NAME);
  private static final long GID = AlluxioFuseUtils.getGid(GROUP_NAME);

  /**
   * Creates a new instance of {@link AlluxioJniFuseFileSystem}.
   *
   * @param fs Alluxio file system
   * @param opts options
   * @param conf Alluxio configuration
   */
  public AlluxioJniFuseFileSystem(
      FileSystem fs, AlluxioFuseOptions opts, AlluxioConfiguration conf) {
    super(Paths.get(opts.getMountPoint()));
    mFsName = conf.get(PropertyKey.FUSE_FS_NAME);
    mFileSystem = fs;
    mConf = conf;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX))
        .build(new PathCacheLoader());
    mIsUserGroupTranslation = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
    for (int i = 0; i < LOCK_SIZE; i++) {
      mFileLocks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * Gets the lock for a particular page. Note that multiple path may share the same lock as lock
   * striping is used to reduce resource overhead for locks.
   *
   * @param fd the file id
   * @return the corresponding page lock
   */
  private ReadWriteLock getFileLock(long fd) {
    return mFileLocks[Math.floorMod((int) fd, LOCK_SIZE)];
  }

  private void setUserGroupIfNeeded(AlluxioURI uri) throws Exception {
    SetAttributePOptions.Builder attributeOptionsBuilder = SetAttributePOptions.newBuilder();
    FuseContext fc = getContext();
    long uid = fc.uid.get();
    long gid = fc.gid.get();
    if (gid != GID) {
      String groupName = AlluxioFuseUtils.getGroupName(gid);
      if (groupName.isEmpty()) {
        // This should never be reached since input gid is always valid
        LOG.error("Failed to get group name from gid {}, fallback to {}.", gid, GROUP_NAME);
        groupName = GROUP_NAME;
      }
      attributeOptionsBuilder.setGroup(groupName);
    }
    if (uid != UID) {
      String userName = AlluxioFuseUtils.getUserName(uid);
      if (userName.isEmpty()) {
        // This should never be reached since input uid is always valid
        LOG.error("Failed to get user name from uid {}, fallback to {}", uid, USER_NAME);
        userName = USER_NAME;
      }
      attributeOptionsBuilder.setOwner(userName);
    }
    SetAttributePOptions setAttributePOptions =  attributeOptionsBuilder.build();
    if (gid != GID || uid != UID) {
      LOG.debug("Set attributes of path {} to {}", uri, setAttributePOptions);
      mFileSystem.setAttribute(uri, setAttributePOptions);
    }
  }

  @Override
  public int create(String path, long mode, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> createInternal(path, mode, fi),
        "create", "path=%s,mode=%o", path, mode);
  }

  private int createInternal(String path, long mode, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create {}: file name longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      FileOutStream os = mFileSystem.createFile(uri,
          CreateFilePOptions.newBuilder()
              .setMode(new Mode((short) mode).toProto())
              .build());
      long fid = mNextOpenFileId.getAndIncrement();
      mCreateFileEntries.put(fid, os);
      fi.fh.set(fid);
      setUserGroupIfNeeded(uri);
    } catch (Throwable e) {
      LOG.error("Failed to getattr {}: ", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    return AlluxioFuseUtils.call(
        LOG, () -> getattrInternal(path, stat), "getattr", "path=%s", path);
  }

  private int getattrInternal(String path, FileStat stat) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      URIStatus status = mFileSystem.getStatus(uri);
      long size = status.getLength();
      stat.st_size.set(size);

      // Sets block number to fulfill du command needs
      // `st_blksize` is ignored in `getattr` according to
      // https://github.com/libfuse/libfuse/blob/d4a7ba44b022e3b63fc215374d87ed9e930d9974/include/fuse.h#L302
      // According to http://man7.org/linux/man-pages/man2/stat.2.html,
      // `st_blocks` is the number of 512B blocks allocated
      stat.st_blocks.set((int) Math.ceil((double) size / 512));

      final long ctime_sec = status.getLastModificationTimeMs() / 1000;
      // Keeps only the "residual" nanoseconds not caputred in citme_sec
      final long ctime_nsec = (status.getLastModificationTimeMs() % 1000) * 1000;

      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);

      if (mIsUserGroupTranslation) {
        // Translate the file owner/group to unix uid/gid
        // Show as uid==-1 (nobody) if owner does not exist in unix
        // Show as gid==-1 (nogroup) if group does not exist in unix
        stat.st_uid.set(AlluxioFuseUtils.getUid(status.getOwner()));
        stat.st_gid.set(AlluxioFuseUtils.getGidFromGroupName(status.getGroup()));
      } else {
        stat.st_uid.set(UID);
        stat.st_gid.set(GID);
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
      LOG.debug("Failed to get info of {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable e) {
      LOG.error("Failed to getattr {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int readdir(String path, long buff, FuseFillDir filter, long offset,
      FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readdirInternal(path, buff, filter, offset, fi),
        "readdir", "path=%s,buf=%s", path, buff);
  }

  private int readdirInternal(String path, long buff, FuseFillDir filter, long offset,
      FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      // standard . and .. entries
//      filter.apply(buff, ".", null, 0);
//      filter.apply(buff, "..", null, 0);

      mFileSystem.iterateStatus(uri, file -> {
        filter.apply(buff, file.getName(), null, 0);
      });
    } catch (Throwable e) {
      LOG.error("Failed to readdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> openInternal(path, fi), "open", "path=%s", path);
  }

  private int openInternal(String path, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      long fd = mNextOpenFileId.getAndIncrement();
      FileInStream is = mFileSystem.openFile(uri);
      mOpenFileEntries.put(fd, is);
      fi.fh.set(fd);
      if (fd % 100 == 1) {
        LOG.info("open(fd={},entries={})", fd, mOpenFileEntries.size());
      }
      return 0;
    } catch (Throwable e) {
      LOG.error("Failed to open {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readInternal(path, buf, size, offset, fi),
        "read", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int readInternal(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    if (mReadOps.incrementAndGet() % 10000 == 500) {
      long cachedBytes =
          MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).getCount();
      long missedBytes =
          MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL.getName()).getCount();
      LOG.info("read: cached {} bytes, missed {} bytes, ratio {}",
          cachedBytes, missedBytes, 1.0 * cachedBytes / (cachedBytes + missedBytes + 1));
    }
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int nread = 0;
    int rd = 0;
    final int sz = (int) size;
    long fd = fi.fh.get();
    // FileInStream is not thread safe
    try (LockResource r1 = new LockResource(getFileLock(fd).writeLock())) {
      FileInStream is = mOpenFileEntries.get(fd);
      if (is == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      is.seek(offset);
      final byte[] dest = new byte[sz];
      while (rd >= 0 && nread < size) {
        rd = is.read(dest, nread, sz - nread);
        if (rd >= 0) {
          nread += rd;
        }
      }

      if (nread == -1) { // EOF
        nread = 0;
      } else if (nread > 0) {
        buf.put(dest, 0, nread);
      }
    } catch (Throwable e) {
      LOG.error("Failed to read {},{},{}: ", path, size, offset, e);
      return -ErrorCodes.EIO();
    }
    return nread;
  }

  @Override
  public int write(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> writeInternal(path, buf, size, offset, fi),
        "write", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int writeInternal(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    final int sz = (int) size;
    final long fd = fi.fh.get();
    FileOutStream os = mCreateFileEntries.get(fd);
    if (os == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (offset < os.getBytesWritten()) {
      // no op
      return sz;
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(dest, 0, sz);
      os.write(dest);
    } catch (IOException e) {
      LOG.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    }
    return sz;
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> flushInternal(path, fi), "flush", "path=%s", path);
  }

  private int flushInternal(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> releaseInternal(path, fi), "release", "path=%s", path);
  }

  private int releaseInternal(String path, FuseFileInfo fi) {
    long fd = fi.fh.get();
    if (mReleaseOps.incrementAndGet() % 100 == 1) {
      LOG.info("release(fd={},entries={})", fd, mOpenFileEntries.size());
    }
    try (LockResource r1 = new LockResource(getFileLock(fd).writeLock())) {
      FileInStream is = mOpenFileEntries.remove(fd);
      FileOutStream os = mCreateFileEntries.remove(fd);
      if (is == null && os == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      if (is != null) {
        is.close();
      }
      if (os != null) {
        os.close();
      }
    } catch (Throwable e) {
      LOG.error("Failed closing {}", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int mkdir(String path, long mode) {
    return AlluxioFuseUtils.call(LOG, () -> mkdirInternal(path, mode),
        "mkdir", "path=%s,mode=%o,", path, mode);
  }

  private int mkdirInternal(String path, long mode) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create directory {}: name longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      mFileSystem.createDirectory(uri,
          CreateDirectoryPOptions.newBuilder()
              .setMode(new Mode((short) mode).toProto())
              .build());
      setUserGroupIfNeeded(uri);
    } catch (Throwable e) {
      LOG.error("Failed to mkdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int unlink(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "unlink", "path=%s", path);
  }

  @Override
  public int rmdir(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "rmdir", "path=%s", path);
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);

    try {
      mFileSystem.delete(uri);
    } catch (Throwable e) {
      LOG.error("Failed to delete {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int rename(String oldPath, String newPath) {
    return AlluxioFuseUtils.call(LOG, () -> renameInternal(oldPath, newPath),
        "rename", "oldPath=%s,newPath=%s,", oldPath, newPath);
  }

  private int renameInternal(String oldPath, String newPath) {
    final AlluxioURI oldUri = mPathResolverCache.getUnchecked(oldPath);
    final AlluxioURI newUri = mPathResolverCache.getUnchecked(newPath);
    final String name = newUri.getName();
    if (name.length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to rename {} to {}, name {} is longer than {} characters",
          oldPath, newPath, name, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      mFileSystem.rename(oldUri, newUri);
    } catch (Throwable e) {
      LOG.error("Failed to rename {} to {}: ", oldPath, newPath, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int chmod(String path, long mode) {
    return AlluxioFuseUtils.call(LOG, () -> chmodInternal(path, mode),
        "chmod", "path=%s,mode=%o", path, mode);
  }

  private int chmodInternal(String path, long mode) {
    AlluxioURI uri = mPathResolverCache.getUnchecked(path);

    SetAttributePOptions options = SetAttributePOptions.newBuilder()
        .setMode(new Mode((short) mode).toProto()).build();
    try {
      mFileSystem.setAttribute(uri, options);
    } catch (Throwable t) {
      LOG.error("Failed to change {} to mode {}", path, mode, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  @Override
  public int chown(String path, long uid, long gid) {
    return AlluxioFuseUtils.call(LOG, () -> chownInternal(path, uid, gid),
        "chown", "path=%s,uid=%o,gid=%o", path, uid, gid);
  }

  private int chownInternal(String path, long uid, long gid) {
    if (!mIsUserGroupTranslation) {
      LOG.info("Cannot change the owner/group of path {}. Please set {} to be true to enable "
              + "user group translation in Alluxio-FUSE.",
          path, PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED.getName());
      return -ErrorCodes.EOPNOTSUPP();
    }

    try {
      SetAttributePOptions.Builder optionsBuilder = SetAttributePOptions.newBuilder();
      final AlluxioURI uri = mPathResolverCache.getUnchecked(path);

      String userName = "";
      if (uid != ID_NOT_SET_VALUE && uid != ID_NOT_SET_VALUE_UNSIGNED) {
        userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EINVAL();
        }
        optionsBuilder.setOwner(userName);
      }

      String groupName = "";
      if (gid != ID_NOT_SET_VALUE && gid != ID_NOT_SET_VALUE_UNSIGNED) {
        groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get group name from gid {}", gid);
          return -ErrorCodes.EINVAL();
        }
        optionsBuilder.setGroup(groupName);
      } else if (!userName.isEmpty()) {
        groupName = AlluxioFuseUtils.getGroupName(userName);
        optionsBuilder.setGroup(groupName);
      }

      if (userName.isEmpty() && groupName.isEmpty()) {
        // This should never be reached
        LOG.info("Unable to change owner and group of file {} when uid is {} and gid is {}", path,
            userName, groupName);
      } else if (userName.isEmpty()) {
        LOG.info("Change group of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, optionsBuilder.build());
      } else {
        LOG.info("Change owner of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, optionsBuilder.build());
      }
    } catch (Throwable t) {
      LOG.error("Failed to chown {} to uid {} and gid {}", path, uid, gid, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  @Override
  public int truncate(String path, long size) {
    LOG.error("Truncate is not supported {}", path);
    return -ErrorCodes.EOPNOTSUPP();
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFileSystemName() {
    return mFsName;
  }

  @Override
  public void mount(boolean blocking, boolean debug, String[] fuseOpts) {
    LOG.info("Mounting AlluxioJniFuseFileSystem: blocking={}, debug={}, fuseOpts=\"{}\"",
        blocking, debug, Arrays.toString(fuseOpts));
    super.mount(blocking, debug, fuseOpts);
    LOG.info("AlluxioJniFuseFileSystem mounted: blocking={}, debug={}, fuseOpts=\"{}\"",
        blocking, debug, Arrays.toString(fuseOpts));
  }

  @Override
  public void umount() {
    LOG.info("Umount AlluxioJniFuseFileSystem, {}",
        ThreadUtils.formatStackTrace(Thread.currentThread()));
    super.umount();
  }

  @VisibleForTesting
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }

  /**
   * Resolves a FUSE path into {@link AlluxioURI} and possibly keeps it in the cache.
   */
  private final class PathCacheLoader extends CacheLoader<String, AlluxioURI> {

    /**
     * Constructs a new {@link PathCacheLoader}.
     */
    public PathCacheLoader() {}

    @Override
    public AlluxioURI load(String fusePath) {
      // fusePath is guaranteed to always be an absolute path (i.e., starts
      // with a fwd slash) - relative to the FUSE mount point
      final String relPath = fusePath.substring(1);
      final Path tpath = mAlluxioRootPath.resolve(relPath);

      return new AlluxioURI(tpath.toString());
    }
  }
}
