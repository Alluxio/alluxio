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
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.auth.AuthPolicyFactory;
import alluxio.fuse.auth.SystemUserGroupAuthPolicy;
import alluxio.fuse.AlluxioFuseOpenUtils.OpenAction;
import alluxio.cli.FuseShell;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.FuseException;
import alluxio.jnifuse.FuseFillDir;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
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
  private final String mMountPoint;
  private final String mFsName;
  private final String mTmpFolder;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;
  // Cache Uid<->Username and Gid<->Groupname mapping for local OS
  private final LoadingCache<String, Long> mUidCache;
  private final LoadingCache<String, Long> mGidCache;
  private final int mMaxUmountWaitTime;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);

  private final Map<Long, FileInStream> mOpenFileEntries = new ConcurrentHashMap<>();
  // Map contains the locks for file opened for reading or writing (OpenAction = READ_WRITE)
  // This is to guarantee open() - concurrent reads()/writes will
  // only create one FileInStream/FileOutStream
  private final Map<Long, Object> mOpenReadWriteLocks = new ConcurrentHashMap<>();
  private final FuseShell mFuseShell;
  private static final IndexDefinition<CreateFileEntry<FileOutStream>, Long>
      ID_INDEX =
      new IndexDefinition<CreateFileEntry<FileOutStream>, Long>(true) {
        @Override
        public Long getFieldValue(CreateFileEntry<FileOutStream> o) {
          return o.getId();
        }
      };

  // Add a PATH_INDEX to know getattr() been called when writing this file
  private static final IndexDefinition<CreateFileEntry<FileOutStream>, String>
      PATH_INDEX =
      new IndexDefinition<CreateFileEntry<FileOutStream>, String>(true) {
        @Override
        public String getFieldValue(CreateFileEntry<FileOutStream> o) {
          return o.getPath();
        }
      };
  private final IndexedSet<CreateFileEntry<FileOutStream>> mCreateFileEntries
      = new IndexedSet<>(ID_INDEX, PATH_INDEX);
  private final boolean mIsUserGroupTranslation;
  private final AuthPolicy mAuthPolicy;

  // Map for holding the async releasing entries for proper umount
  private final Map<Long, FileInStream> mReleasingReadEntries = new ConcurrentHashMap<>();
  private final Map<Long, CreateFileEntry<FileOutStream>> mReleasingWriteEntries =
      new ConcurrentHashMap<>();

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
    mMountPoint = opts.getMountPoint();
    mTmpFolder = conf.get(PropertyKey.FUSE_TMP_FOLDER);
    mFuseShell = new FuseShell(fs, conf);
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
            long uid = AlluxioFuseUtils.getUid(userName);
            return uid == -1 ? SystemUserGroupAuthPolicy.DEFAULT_UID : uid;
          }
        });
    mGidCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<String, Long>() {
          @Override
          public Long load(String groupName) {
            long gid = AlluxioFuseUtils.getGidFromGroupName(groupName);
            return gid == -1 ? SystemUserGroupAuthPolicy.DEFAULT_GID : gid;
          }
        });
    mIsUserGroupTranslation = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
    mMaxUmountWaitTime = (int) conf.getMs(PropertyKey.FUSE_UMOUNT_TIMEOUT);
    mAuthPolicy = AuthPolicyFactory.create(mFileSystem, conf, this);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.FUSE_READING_FILE_COUNT.getName()),
        mOpenFileEntries::size);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.FUSE_WRITING_FILE_COUNT.getName()),
        mCreateFileEntries::size);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.FUSE_CACHED_PATH_COUNT.getName()),
        mPathResolverCache::size);
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
      mAuthPolicy.setUserGroupIfNeeded(uri);
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
      URIStatus status = null;
      // Handle special metadata cache operation
      if (mConf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED)
          && mFuseShell.isSpecialCommand(uri)) {
        // TODO(lu) add cache for isFuseSpecialCommand if needed
        status = mFuseShell.runCommand(uri);
      } else {
        status = mFileSystem.getStatus(uri);
      }
      long size = status.getLength();
      if (!status.isCompleted()) {
        if (mCreateFileEntries.contains(PATH_INDEX, path)) {
          // Alluxio master will not update file length until file is completed
          // get file length from the current output stream
          CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(PATH_INDEX, path);
          if (ce != null) {
            FileOutStream os = ce.getOut();
            size = os.getBytesWritten();
          }
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

      if (mIsUserGroupTranslation) {
        // Translate the file owner/group to unix uid/gid
        // Show as uid==-1 (nobody) if owner does not exist in unix
        // Show as gid==-1 (nogroup) if group does not exist in unix
        stat.st_uid.set(mUidCache.get(status.getOwner()));
        stat.st_gid.set(mGidCache.get(status.getGroup()));
      } else {
        stat.st_uid.set(SystemUserGroupAuthPolicy.DEFAULT_UID);
        stat.st_gid.set(SystemUserGroupAuthPolicy.DEFAULT_GID);
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
    } catch (AccessControlException e) {
      LOG.error("Permission denied when getattr {}: ", path, e);
      return -ErrorCodes.EACCES();
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
    final int flags = fi.flags.get();
    return AlluxioFuseUtils.call(LOG, () -> openInternal(path, fi),
        "Fuse.open", "path=%s,flags=0x%x", path, flags);
  }

  private int openInternal(String path, FuseFileInfo fi) {
    final int flags = fi.flags.get();
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    OpenAction openAction = AlluxioFuseOpenUtils.getOpenAction(flags);
    if (openAction == OpenAction.UNKNOWN) {
      LOG.error(String.format("Unknown open flag 0x%x for path %s, "
          + "please raise a ticket at https://github.com/Alluxio/alluxio/issues", flags, path));
      return -ErrorCodes.EOPNOTSUPP();
    }
    if (openAction == OpenAction.NOT_SUPPORTED) {
      LOG.error(String.format("Not supported open flag 0x%x for path %s. "
          + "Alluxio does not support file modification. "
          + "Cannot open directory in fuse.open().",
          flags, path));
      return -ErrorCodes.EOPNOTSUPP();
    }
    long fd = mNextOpenFileId.getAndIncrement();
    URIStatus status = null;
    try {
      status = mFileSystem.getStatus(uri);
    } catch (InvalidPathException | FileNotFoundException e) {
      status = null;
    } catch (Throwable t) {
      LOG.error("Failed to get status of path {} when opening it.", path);
      return -ErrorCodes.EIO();
    }
    if (status != null && !status.isCompleted()) {
      // Cannot open incomplete file for read or write
      // wait for file to complete in read or read_write mode
      if (openAction == OpenAction.READ_ONLY
          || openAction == OpenAction.READ_WRITE) {
        if (!AlluxioFuseUtils.waitForFileCompleted(mFileSystem, uri)) {
          LOG.error(String.format("Cannot open incomplete file %s. "
              + "Failed to wait for file completed with flag 0x%x",
              path, flags));
          return -ErrorCodes.EIO();
        }
      } else if (openAction == OpenAction.WRITE_ONLY) {
        LOG.error("Cannot open incomplete file {} for writing", path);
        return -ErrorCodes.EOPNOTSUPP();
      }
    }

    fi.fh.set(fd);
    try {
      if (openAction == OpenAction.WRITE_ONLY) {
        if (status != null) {
          OpenFlags openFlags = OpenFlags.valueOf(flags);
          if (openFlags == OpenFlags.O_CREAT || openFlags == OpenFlags.O_EXCL) {
            return -ErrorCodes.EEXIST();
          }
          mFileSystem.delete(uri);
        }
        FileOutStream os = mFileSystem.createFile(uri);
        mCreateFileEntries.add(new CreateFileEntry(fd, path, os));
        mAuthPolicy.setUserGroupIfNeeded(uri);
        LOG.debug(String.format("Open path %s with flags 0x%x for overwriting. "
                + "Alluxio deleted the old file and created a new file for writing",
            path, flags));
      } else if (openAction == OpenAction.READ_ONLY) {
        FileInStream is;
        is = mFileSystem.openFile(uri);
        mOpenFileEntries.put(fd, is);
      } else {
        // For OpenAction.READ_WRITE, we defer to the first read() or write()
        // to open or create file.
        Object lock = new Object();
        mOpenReadWriteLocks.put(fd, lock);
      }
      return 0;
    } catch (Throwable e) {
      LOG.error("Failed to open path={},openAction={}: ", path, openAction, e);
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
    final int sz = (int) size;
    int nread = 0;
    int rd = 0;
    final long fd = fi.fh.get();
    try {
      FileInStream is = mOpenFileEntries.get(fd);
      if (is == null) {
        // Could be first read in open(READ_WRITE)
        Object lock = mOpenReadWriteLocks.get(fd);
        if (lock == null) {
          // The FileInStream can be created by other
          // concurrent read() operations
          is = mOpenFileEntries.get(fd);
          if (is == null) {
            LOG.error("Cannot find fd for {} in table", path);
            return -ErrorCodes.EBADFD();
          }
        }
        if (is == null) {
          synchronized (lock) {
            is = mOpenFileEntries.get(fd);
            CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
            if (ce != null) {
              LOG.error("Cannot open file {} for concurrent read and write", path);
              return -ErrorCodes.EOPNOTSUPP();
            }
            if (is == null) {
              try {
                final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
                is = mFileSystem.openFile(uri);
                mOpenFileEntries.put(fd, is);
              } finally {
                mOpenReadWriteLocks.remove(fd);
              }
            }
          }
        }
      }

      // FileInStream is not thread safe
      synchronized (is) {
        if (!mOpenFileEntries.containsKey(fd)) {
          LOG.error("Cannot find fd {} for {}", fd, path);
          return -ErrorCodes.EBADFD();
        }
        if (offset - is.getPos() < is.remaining()) {
          is.seek(offset);
          while (rd >= 0 && nread < sz) {
            rd = is.read(buf, nread, sz - nread);
            if (rd >= 0) {
              nread += rd;
            }
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
    final int flags = fi.flags.get();
    CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
    if (ce == null) {
      // Check if file is opened for OpenAction=READ_WRITE.
      // The file is opened for either read or write.
      // Read or write depends on the first operation
      Object lock = mOpenReadWriteLocks.get(fd);
      if (lock == null) {
        // Write is usually sequential
        // Used to prevent concurrent write in the future
        ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
        if (ce == null) {
          LOG.error("Cannot find fd for {} in table", path);
          return -ErrorCodes.EBADFD();
        }
      }
      if (ce == null) {
        if (offset != 0) {
          LOG.error(String.format("Cannot overwrite file %s with offset %s. "
              + "File is opened with flags 0x%x", path, offset, fi.flags.get()));
          return -ErrorCodes.EIO();
        }
        synchronized (lock) {
          ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
          FileInStream is = mOpenFileEntries.get(fd);
          if (is != null) {
            LOG.error("Cannot open file {} for concurrent read and write", path);
            return -ErrorCodes.EOPNOTSUPP();
          }
          if (ce == null) {
            try {
              final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
              if (mFileSystem.exists(uri)) {
                mFileSystem.delete(uri);
              }
              FileOutStream os = mFileSystem.createFile(uri);
              ce = new CreateFileEntry(fd, path, os);
              mCreateFileEntries.add(ce);
              mAuthPolicy.setUserGroupIfNeeded(uri);
              LOG.debug(String.format("Open path %s with flags 0x%x for reading and writing. "
                      + "Treat as write only and error out if detecting reading behavior. "
                      + "Alluxio deleted the old file and created a new file for writing",
                  path, flags));
            } catch (Throwable e) {
              LOG.error(String.format("Failed to write path: %s size: %s offset: %s flags: 0x%x",
                  path, size, offset, flags), e);
              return -ErrorCodes.EIO();
            } finally {
              mOpenReadWriteLocks.remove(fd);
            }
          }
        }
      }
    }
    if (!mTmpFolder.isEmpty()) {
      RandomAccessFile tmpFile;
      try {
        File tmpFolder = new File(mTmpFolder);
        if (!tmpFolder.exists()) {
          tmpFolder.mkdirs();
        }
        tmpFile = new RandomAccessFile(Paths.get(mTmpFolder, path).toString(), "rw");
      } catch (FileNotFoundException e) {
        LOG.error("Failed to create temporary file {}: ", Paths.get(mTmpFolder, path), e);
        return -ErrorCodes.EIO();
      }
      try {
        final byte[] dest = new byte[sz];
        buf.get(dest, 0, sz);
        tmpFile.seek(offset);
        tmpFile.write(dest);
        tmpFile.close();
      } catch (IOException e) {
        LOG.error("IOException while writing to {}.", path, e);
        return -ErrorCodes.EIO();
      }
    } else {
      FileOutStream os = ce.getOut();
      long bytesWritten = os.getBytesWritten();
      if (offset != bytesWritten && offset + sz > bytesWritten) {
        LOG.error("Only sequential write is supported. Cannot write bytes of size {} to offset {} "
                      + "when {} bytes have written to path {}", size, offset, bytesWritten, path);
        return -ErrorCodes.EIO();
      }
      if (offset + sz <= bytesWritten) {
        LOG.warn("Skip writting to file {} offset={} size={} when {} bytes has written to file",
            path, offset, sz, bytesWritten);
        // To fulfill vim :wq
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
    CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
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
      CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(ID_INDEX, fd);
      if (!mTmpFolder.isEmpty() && ce != null) {
        FileOutStream os = ce.getOut();
        FileInputStream tmpFileInputStream = new FileInputStream(
            Paths.get(mTmpFolder, path).toString());
        byte[] buf = new byte[64 * 1024];
        int bytesRead = 0;
        while (bytesRead != -1) {
          bytesRead = tmpFileInputStream.read(buf);
          if (bytesRead > 0) {
            os.write(buf, 0, bytesRead);
          }
        }
        tmpFileInputStream.close();
        Files.delete(Paths.get(mTmpFolder, path));
      }
      mOpenReadWriteLocks.remove(fd);
      FileInStream is = mOpenFileEntries.remove(fd);
      if (is == null && ce == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      if (ce != null) {
        // Remove earlier to try best effort to avoid write() - async release() - getAttr()
        // without waiting for file completed and return 0 bytes file size error
        mCreateFileEntries.remove(ce);
        mReleasingWriteEntries.put(fd, ce);
        try {
          synchronized (ce) {
            ce.close();
          }
        } finally {
          mReleasingWriteEntries.remove(fd);
        }
      }
      if (is != null) {
        mReleasingReadEntries.put(fd, is);
        try {
          synchronized (is) {
            is.close();
          }
        } finally {
          mReleasingReadEntries.remove(fd);
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
      mAuthPolicy.setUserGroupIfNeeded(uri);
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
      CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(PATH_INDEX, oldPath);
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
    // Truncate scenarios:
    // 1. Truncate size = 0, file does not exist => no-op
    // 2. Truncate size = 0, file exists and completed
    // => noop if file size = 0 or delete file
    // TODO(lu) delete open file entry if any
    // 3. Truncate size = 0, file exists and is being written by current Fuse
    // => noop if written size = 0, otherwise delete file and update create file entry
    // 4. Truncate size = 0, file exists and is being written by other applications
    // => error out
    // 5. Truncate size != 0, file does not exist => error out
    // 6. Truncate size != 0, file is completed
    // => no-op if file size = truncate size, error out otherwise
    // 7. Truncate size != 0, file is being written by other applications
    // => error out, don't know exact file written size
    // 8. Truncate size != 0, file is being written by current Fuse
    // => no-op if written size = truncate size, error out otherwise
    // Error out in all other cases
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    URIStatus status = null;
    try {
      status = mFileSystem.getStatus(uri);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      status = null;
    } catch (Throwable t) {
      LOG.error("Failed to truncate path {} to {}. Failed to get file status", path, size, t);
      return -ErrorCodes.EIO();
    }
    CreateFileEntry<FileOutStream> ce = mCreateFileEntries.getFirstByField(PATH_INDEX, path);
    if (size == 0) {
      if (status == null) {
        // Case 1: Truncate size = 0, file does not exist => no-op
        return 0;
      }
      try {
        // Case 2: Truncate size = 0, file exists and completed
        if (status.isCompleted()) {
          if (status.getLength() == 0) {
            return 0;
          }
          mFileSystem.delete(uri);
          // TODO(lu) delete existing opened file entry if any
          // require libfuse 3 which truncate() has fd info
          // otherwise we need to change OpenFileEntries to include PATH_INDEX
          // now fuse.read() will error out but it's acceptable
          // because concurrent read and delete are not supported
          return 0;
        }
        // Case 3: Truncate size = 0, file exists and is being written by current Fuse
        if (ce != null) {
          FileOutStream os = ce.getOut();
          long bytesWritten = os.getBytesWritten();
          if (bytesWritten == 0) {
            return 0;
          }
          mCreateFileEntries.remove(ce);
          mFileSystem.delete(uri);
          os = mFileSystem.createFile(uri);
          final long fd = ce.getId();
          ce = new CreateFileEntry(fd, path, os);
          mCreateFileEntries.add(ce);
          mAuthPolicy.setUserGroupIfNeeded(uri);
          return 0;
        }
        // Case 4: Truncate size = 0, file exists and is being written by other applications
        LOG.error("Failed to truncate path {} to size 0, "
            + "file is being written in other applications",
            path);
        return -ErrorCodes.EOPNOTSUPP();
      } catch (Throwable t) {
        LOG.error("Failed to truncate path {} to {}. Failed to delete path {} from Alluxio",
            path, size, path);
        return -ErrorCodes.EIO();
      }
    }
    // Case 5: Truncate size != 0, file does not exist => error out
    if (status == null) {
      LOG.error("Cannot truncate non-existing file {} to size {}", path, size);
      return -ErrorCodes.EEXIST();
    }
    // Case 6: Truncate size != 0, file is completed
    if (status.isCompleted()) {
      if (size == status.getLength()) {
        return 0;
      }
      LOG.error("Cannot truncate file {} to non-zero size {}", path, size);
      return -ErrorCodes.EOPNOTSUPP();
    }
    // Case 7: Truncate size != 0, file is being written by other applications
    if (ce == null) {
      LOG.error("Cannot truncate {} to {}. "
          + "File is being written in other Fuse applications or APIs",
          path, size);
      return -ErrorCodes.EOPNOTSUPP();
    }
    // Case 8: Truncate size != 0, file is being written by current Fuse
    FileOutStream os = ce.getOut();
    long bytesWritten = os.getBytesWritten();
    if (bytesWritten == size) {
      return 0;
    }
    // error out otherwise
    LOG.error("Truncate file {} of size {} to size {} is not supported. "
        + "Alluxio supports sequential writes only and the written contents cannot be modified",
        path, bytesWritten, size);
    return -ErrorCodes.EOPNOTSUPP();
  }

  @Override
  public int utimensCallback(String path, long aSec, long aNsec, long mSec, long mNsec) {
    // TODO(maobaolong): implements this logic for alluxio.
    LOG.debug("utimens for {}, but do nothing for this filesystem", path);
    return 0;
  }

  @Override
  public int symlink(String linkname, String path) {
    LOG.warn("Not supported symlink operation, linkname {}, path{}", linkname, path);
    return -ErrorCodes.ENOTSUP();
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFileSystemName() {
    return mFsName;
  }

  @Override
  public void umount(boolean force) throws FuseException {
    // Release operation is async, we will try our best efforts to
    // close all opened file in/out stream before umounting the fuse
    if (!mCreateFileEntries.isEmpty() || !mOpenFileEntries.isEmpty()) {
      LOG.info("Unmounting {}. Waiting for all in progress file read/write to finish", mMountPoint);
      try {
        CommonUtils.waitFor("all in progress file read/write to finish",
            () -> mCreateFileEntries.isEmpty() && mOpenFileEntries.isEmpty(),
                WaitForOptions.defaults().setTimeoutMs(mMaxUmountWaitTime));
      } catch (InterruptedException e) {
        LOG.error("Unmount {} interrupted", mMountPoint);
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        LOG.error("Timeout when waiting all in progress file read/write to finish "
            + "when unmounting {}. {} fileInStream remain unclosed. "
            + "{} fileOutStream remain unclosed.",
            mMountPoint, mOpenFileEntries.size(), mCreateFileEntries.size());
      }
    }

    // Waiting for in progress async release to finish
    if (!mReleasingReadEntries.isEmpty() || !mReleasingWriteEntries.isEmpty()) {
      LOG.info("Unmounting {}. Waiting for all in progress file read/write closing to finish",
          mMountPoint);
      try {
        CommonUtils.waitFor("all in progress file read/write closing to finish",
            () -> mReleasingReadEntries.isEmpty() && mReleasingWriteEntries.isEmpty(),
            WaitForOptions.defaults().setTimeoutMs(mMaxUmountWaitTime));
      } catch (InterruptedException e) {
        LOG.error("Unmount {} interrupted", mMountPoint);
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        LOG.error("Timeout when waiting in progress file read/write closing to finish "
            + "when unmounting {}. {} fileInStream and {} fileOutStream "
            + "are still in closing process.",
            mMountPoint, mReleasingReadEntries.size(), mReleasingWriteEntries.size());
      }
    }

    if (!(mCreateFileEntries.isEmpty() && mOpenFileEntries.isEmpty())) {
      // TODO(lu) consider the case that client application may not call release()
      // for all open() or create(). Force closing those operations.
      // TODO(lu,bin) properly prevent umount when device is busy
      LOG.error("Unmounting {} when device is busy in reading/writing files. "
          + "{} fileInStream and {} fileOutStream remain open.",
          mMountPoint, mCreateFileEntries.size(), mOpenFileEntries.size());
      if (!force) {
        throw new FuseException("Timed out for umount due to device is busy.");
      }
    }
    super.umount(force);
  }

  @VisibleForTesting
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }
}
