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
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
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
import alluxio.security.authorization.Mode;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
  private final AlluxioConfiguration mConf;
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;
  private final LoadingCache<String, Long> mUidCache;
  private final LoadingCache<String, Long> mGidCache;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final String mFsName;

  private final Map<Long, FileInStream> mOpenFileEntries = new ConcurrentHashMap<>();

  private static final IndexDefinition<CreateFileEntry, Long>
      ID_INDEX =
      new IndexDefinition<CreateFileEntry, Long>(true) {
        @Override
        public Long getFieldValue(CreateFileEntry o) {
          return o.getId();
        }
      };

  // Add a PATH_INDEX to know getattr() been called when writing this file
  private static final IndexDefinition<CreateFileEntry, String>
      PATH_INDEX =
      new IndexDefinition<CreateFileEntry, String>(true) {
        @Override
        public String getFieldValue(CreateFileEntry o) {
          return o.getPath();
        }
      };
  private final IndexedSet<CreateFileEntry> mCreateFileEntries
      = new IndexedSet<>(ID_INDEX, PATH_INDEX);
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
  private static final long DEFAULT_UID = AlluxioFuseUtils.getUid(USER_NAME);
  private static final long DEFAULT_GID = AlluxioFuseUtils.getGid(GROUP_NAME);

  /**
   * Creates a new instance of {@link AlluxioJniFuseFileSystem}.
   *
   * @param fs Alluxio file system
   * @param opts options
   * @param conf Alluxio configuration
   */
  public AlluxioJniFuseFileSystem(
      FileSystem fs, FuseMountOptions opts, AlluxioConfiguration conf) {
    super(Paths.get(opts.getMountPoint()));
    mFsName = conf.get(PropertyKey.FUSE_FS_NAME);
    mFileSystem = fs;
    mConf = conf;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX))
        .build(new CacheLoader<String, AlluxioURI>() {
          @Override
          public AlluxioURI load(String fusePath) {
            // fusePath is guaranteed to always be an absolute path (i.e., starts
            // with a fwd slash) - relative to the FUSE mount point
            final String relPath = fusePath.substring(1);
            final Path tpath = mAlluxioRootPath.resolve(relPath);
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
    mIsUserGroupTranslation = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
  }

  private void setUserGroupIfNeeded(AlluxioURI uri) throws Exception {
    SetAttributePOptions.Builder attributeOptionsBuilder = SetAttributePOptions.newBuilder();
    FuseContext fc = getContext();
    long uid = fc.uid.get();
    long gid = fc.gid.get();
    if (gid != DEFAULT_GID) {
      String groupName = AlluxioFuseUtils.getGroupName(gid);
      if (groupName.isEmpty()) {
        // This should never be reached since input gid is always valid
        LOG.error("Failed to get group name from gid {}, fallback to {}.", gid, GROUP_NAME);
        groupName = GROUP_NAME;
      }
      attributeOptionsBuilder.setGroup(groupName);
    }
    if (uid != DEFAULT_UID) {
      String userName = AlluxioFuseUtils.getUserName(uid);
      if (userName.isEmpty()) {
        // This should never be reached since input uid is always valid
        LOG.error("Failed to get user name from uid {}, fallback to {}", uid, USER_NAME);
        userName = USER_NAME;
      }
      attributeOptionsBuilder.setOwner(userName);
    }
    SetAttributePOptions setAttributePOptions =  attributeOptionsBuilder.build();
    if (gid != DEFAULT_GID || uid != DEFAULT_UID) {
      LOG.debug("Set attributes of path {} to {}", uri, setAttributePOptions);
      mFileSystem.setAttribute(uri, setAttributePOptions);
    }
  }

  @Override
  public int create(String path, long mode, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> createInternal(path, mode, fi),
        "Fuse.Create", "path=%s,mode=%o", path, mode);
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
      mCreateFileEntries.add(new CreateFileEntry(fid, path, os));
      fi.fh.set(fid);
      setUserGroupIfNeeded(uri);
    } catch (Throwable e) {
      LOG.error("Failed to create {}: ", path, e);
      return -ErrorCodes.EIO();
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
    try {
      URIStatus status = mFileSystem.getStatus(uri);
      if (!status.isCompleted()) {
        // Always block waiting for file to be completed except when the file is writing
        // We do not want to block the writing process
        if (!mCreateFileEntries.contains(PATH_INDEX, path)
            && !AlluxioFuseUtils.waitForFileCompleted(mFileSystem, uri)) {
          LOG.error("File {} is not completed", path);
        }
        status = mFileSystem.getStatus(uri);
      }
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
      final long ctime_nsec = (status.getLastModificationTimeMs() % 1000) * 1_000_000L;

      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);

      if (mIsUserGroupTranslation) {
        // Translate the file owner/group to unix uid/gid
        // Show as uid==-1 (nobody) if owner does not exist in unix
        // Show as gid==-1 (nogroup) if group does not exist in unix
        stat.st_uid.set(mUidCache.get(status.getOwner()));
        stat.st_gid.set(mGidCache.get(status.getGroup()));
      } else {
        stat.st_uid.set(DEFAULT_UID);
        stat.st_gid.set(DEFAULT_GID);
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
  public int readdir(String path, long buff, long filter, long offset,
      FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readdirInternal(path, buff, filter, offset, fi),
        "Fuse.Readdir", "path=%s,buf=%s", path, buff);
  }

  private int readdirInternal(String path, long buff, long filter, long offset,
      FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      // standard . and .. entries
      FuseFillDir.apply(filter, buff, ".", null, 0);
      FuseFillDir.apply(filter, buff, "..", null, 0);

      mFileSystem.iterateStatus(uri, file -> {
        FuseFillDir.apply(filter, buff, file.getName(), null, 0);
      });
    } catch (Throwable e) {
      LOG.error("Failed to readdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> openInternal(path, fi), "Fuse.Open", "path=%s", path);
  }

  private int openInternal(String path, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      long fd = mNextOpenFileId.getAndIncrement();
      FileInStream is;
      try {
        is = mFileSystem.openFile(uri);
      } catch (FileIncompleteException e) {
        if (AlluxioFuseUtils.waitForFileCompleted(mFileSystem, uri)) {
          is = mFileSystem.openFile(uri);
        } else {
          throw e;
        }
      }
      mOpenFileEntries.put(fd, is);
      fi.fh.set(fd);
      return 0;
    } catch (Throwable e) {
      LOG.error("Failed to open {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readInternal(path, buf, size, offset, fi),
        "Fuse.Read", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int readInternal(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    MetricsSystem.counter(MetricKey.FUSE_BYTES_TO_READ.getName()).inc(size);
    int nread = 0;
    int rd = 0;
    long fd = fi.fh.get();
    try {
      FileInStream is = mOpenFileEntries.get(fd);
      if (is == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      // FileInStream is not thread safe
      synchronized (is) {
        if (!mOpenFileEntries.containsKey(fd)) {
          LOG.error("Cannot find fd {} for {}", fd, path);
          return -ErrorCodes.EBADFD();
        }
        if (offset - is.getPos() < is.remaining()) {
          is.seek(offset);
          final int sz = (int) size;
          final byte[] dest = new byte[sz];
          while (rd >= 0 && nread < sz) {
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
        }
      }
    } catch (Throwable e) {
      LOG.error("Failed to read, path: {} size: {} offset: {}", path, size, offset, e);
      return -ErrorCodes.EIO();
    }
    MetricsSystem.counter(MetricKey.FUSE_BYTES_READ.getName()).inc(nread);
    return nread;
  }

  @Override
  public int write(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> writeInternal(path, buf, size, offset, fi),
        "Fuse.Write", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int writeInternal(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    final int sz = (int) size;
    final long fd = fi.fh.get();
    CreateFileEntry ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
    if (ce == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    FileOutStream os = ce.getOut();
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
    return AlluxioFuseUtils.call(LOG, () -> flushInternal(path, fi), "Fuse.Flush", "path=%s", path);
  }

  private int flushInternal(String path, FuseFileInfo fi) {
    final long fd = fi.fh.get();

    FileInStream is = mOpenFileEntries.get(fd);
    CreateFileEntry ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
    if (ce == null && is == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (ce == null) {
      // flush() may be called in places other than write
      return 0;
    }

    try {
      synchronized (ce) {
        ce.getOut().flush();
      }
    } catch (Throwable e) {
      LOG.error("Failed to flush {}", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> releaseInternal(path, fi),
        "Fuse.Release", "path=%s", path);
  }

  private int releaseInternal(String path, FuseFileInfo fi) {
    long fd = fi.fh.get();
    try {
      FileInStream is = mOpenFileEntries.remove(fd);
      CreateFileEntry ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
      if (is == null && ce == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      if (is != null) {
        synchronized (is) {
          is.close();
        }
      }
      if (ce != null) {
        mCreateFileEntries.remove(ce);
        synchronized (ce) {
          ce.getOut().close();
        }
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
        "Fuse.Mkdir", "path=%s,mode=%o,", path, mode);
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
        "Fuse.Rename", "oldPath=%s,newPath=%s,", oldPath, newPath);
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
      CreateFileEntry ce = mCreateFileEntries.getFirstByField(PATH_INDEX, oldPath);
      if (ce != null) {
        ce.setPath(newPath);
      }
    } catch (Throwable e) {
      LOG.error("Failed to rename {} to {}: ", oldPath, newPath, e);
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
        "Fuse.Chown", "path=%s,uid=%o,gid=%o", path, uid, gid);
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
        LOG.info("Change owner of file {} to {}", path, userName);
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

  @VisibleForTesting
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }
}
