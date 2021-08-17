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
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockMasterInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jnr.ffi.Pointer;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.uid_t;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseContext;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse.
 */
@ThreadSafe
public final class AlluxioFuseFileSystem extends FuseStubFS
    implements FuseUmountable {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseFileSystem.class);
  private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
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

  private static InstancedConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  /**
   * 4294967295 is unsigned long -1, -1 means that uid or gid is not set.
   * 4294967295 or -1 occurs when chown without user name or group name.
   * Please view https://github.com/SerCeMan/jnr-fuse/issues/67 for more details.
   */
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE = -1;
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;

  private static final long UID = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
  private static final long GID = AlluxioFuseUtils.getGid(System.getProperty("user.name"));

  // Open file managements
  private static final IndexDefinition<OpenFileEntry<FileInStream, FileOutStream>, Long>
      ID_INDEX =
      new IndexDefinition<OpenFileEntry<FileInStream, FileOutStream>, Long>(true) {
        @Override
        public Long getFieldValue(OpenFileEntry o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<OpenFileEntry<FileInStream, FileOutStream>, String>
      PATH_INDEX =
      new IndexDefinition<OpenFileEntry<FileInStream, FileOutStream>, String>(true) {
        @Override
        public String getFieldValue(OpenFileEntry o) {
          return o.getPath();
        }
      };

  private final boolean mIsUserGroupTranslation;
  private final FileSystem mFileSystem;
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;

  // Table of open files with corresponding InputStreams and OutputStreams
  private final IndexedSet<OpenFileEntry<FileInStream, FileOutStream>> mOpenFiles;

  private AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final String mFsName;

  /**
   * Creates a new instance of {@link AlluxioFuseFileSystem}.
   *
   * @param fs Alluxio file system
   * @param opts options
   * @param conf Alluxio configuration
   */
  public AlluxioFuseFileSystem(FileSystem fs, FuseMountOptions opts, AlluxioConfiguration conf) {
    super();
    mFsName = conf.get(PropertyKey.FUSE_FS_NAME);
    mFileSystem = fs;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mOpenFiles = new IndexedSet<>(ID_INDEX, PATH_INDEX);

    final int maxCachedPaths = conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    mIsUserGroupTranslation
        = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(maxCachedPaths)
        .build(new PathCacheLoader());

    Preconditions.checkArgument(mAlluxioRootPath.isAbsolute(),
        "alluxio root path should be absolute");
  }

  /**
   * Changes the mode of an Alluxio file.
   *
   * @param path the path of the file
   * @param mode the mode to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chmod(String path, @mode_t long mode) {
    return AlluxioFuseUtils.call(LOG, () -> chmodInternal(path, mode),
        "chmod", "path=%s,mode=%o", path, mode);
  }

  private int chmodInternal(String path, @mode_t long mode) {
    AlluxioURI uri = mPathResolverCache.getUnchecked(path);

    SetAttributePOptions options = SetAttributePOptions.newBuilder()
        .setMode(new alluxio.security.authorization.Mode((short) mode).toProto()).build();
    try {
      mFileSystem.setAttribute(uri, options);
    } catch (Throwable t) {
      LOG.error("Failed to change {} to mode {}", path, mode, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  /**
   * Changes the user and group ownership of an Alluxio file.
   * This operation only works when the user group translation is enabled in Alluxio-FUSE.
   *
   * @param path the path of the file
   * @param uid the uid to change to
   * @param gid the gid to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chown(String path, @uid_t long uid, @gid_t long gid) {
    return AlluxioFuseUtils.call(LOG, () -> chownInternal(path, uid, gid),
        "chown", "path=%s,uid=%o,gid=%o", path, uid, gid);
  }

  private int chownInternal(String path, @uid_t long uid, @gid_t long gid) {
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
   * Creates and opens a new file.
   *
   * @param path The FS path of the file to open
   * @param mode mode flags
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success. A negative value on error
   */
  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> createInternal(path, mode, fi),
        "create", "path=%s,mode=%o", path, mode);
  }

  private int createInternal(String path, @mode_t long mode, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create {}, file name is longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      if (mOpenFiles.size() >= MAX_OPEN_FILES) {
        LOG.error("Cannot create {}: too many open files (MAX_OPEN_FILES: {})", path,
            MAX_OPEN_FILES);
        return -ErrorCodes.EMFILE();
      }
      SetAttributePOptions.Builder attributeOptionsBuilder = SetAttributePOptions.newBuilder();
      FuseContext fc = getContext();
      long uid = fc.uid.get();
      long gid = fc.gid.get();

      if (gid != GID) {
        String groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached since input gid is always valid
          LOG.error("Failed to get group name from gid {}.", gid);
          return -ErrorCodes.EFAULT();
        }
        attributeOptionsBuilder.setGroup(groupName);
      }
      if (uid != UID) {
        String userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached since input uid is always valid
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EFAULT();
        }
        attributeOptionsBuilder.setOwner(userName);
      }
      SetAttributePOptions setAttributePOptions = attributeOptionsBuilder.build();
      FileOutStream os = mFileSystem.createFile(uri,
          CreateFilePOptions.newBuilder()
              .setMode(new alluxio.security.authorization.Mode((short) mode).toProto())
              .build());
      long fid = mNextOpenFileId.getAndIncrement();
      mOpenFiles.add(new OpenFileEntry(fid, path, null, os));
      fi.fh.set(fid);
      if (gid != GID || uid != UID) {
        LOG.debug("Set attributes of path {} to {}", path, setAttributePOptions);
        mFileSystem.setAttribute(uri, setAttributePOptions);
      }
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to create {}, file already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Failed to create {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to create {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Flushes cached data on Alluxio.
   *
   * Called on explicit sync() operation or at close().
   *
   * @param path The path on the FS of the file to close
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int flush(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> flushInternal(path, fi), "flush", "path=%s", path);
  }

  private int flushInternal(String path, FuseFileInfo fi) {
    final long fd = fi.fh.get();
    OpenFileEntry oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (oe.getOut() != null) {
      try {
        oe.getOut().flush();
      } catch (IOException e) {
        LOG.error("Failed to flush {}", path, e);
        return -ErrorCodes.EIO();
      }
    } else {
      LOG.debug("Not flushing: {} was not open for writing", path);
    }
    return 0;
  }

  /**
   * Retrieves file attributes.
   *
   * @param path The path on the FS of the file
   * @param stat FUSE data structure to fill with file attrs
   * @return 0 on success, negative value on error
   */
  @Override
  public int getattr(String path, FileStat stat) {
    return AlluxioFuseUtils.call(
        LOG, () -> getattrInternal(path, stat), "getattr", "path=%s", path);
  }

  private int getattrInternal(String path, FileStat stat) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    try {
      URIStatus status = mFileSystem.getStatus(turi);
      if (!status.isCompleted()) {
        // Always block waiting for file to be completed except when the file is writing
        // We do not want to block the writing process
        if (!mOpenFiles.contains(PATH_INDEX, path)
            && !AlluxioFuseUtils.waitForFileCompleted(mFileSystem, turi)) {
          LOG.error("File {} is not completed", path);
        }
        status = mFileSystem.getStatus(turi);
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
    } catch (Throwable t) {
      LOG.error("Failed to get info of {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFSName() {
    return mFsName;
  }

  /**
   * Creates a new dir.
   *
   * @param path the path on the FS of the new dir
   * @param mode Dir creation flags (IGNORED)
   * @return 0 on success, a negative value on error
   */
  @Override
  public int mkdir(String path, @mode_t long mode) {
    return AlluxioFuseUtils.call(LOG, () -> mkdirInternal(path, mode),
        "mkdir", "path=%s,mode=%o,", path, mode);
  }

  private int mkdirInternal(String path, @mode_t long mode) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    if (turi.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create directory {}, directory name is longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    SetAttributePOptions.Builder attributeOptionsBuilder = SetAttributePOptions.newBuilder();
    FuseContext fc = getContext();
    long uid = fc.uid.get();
    long gid = fc.gid.get();
    try {
      if (gid != GID) {
        String groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached since input gid is always valid
          LOG.error("Failed to get group name from gid {}.", gid);
          return -ErrorCodes.EFAULT();
        }
        attributeOptionsBuilder.setGroup(groupName);
      }
      if (uid != UID) {
        String userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached since input uid is always valid
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EFAULT();
        }
        attributeOptionsBuilder.setOwner(userName);
      }
      SetAttributePOptions setAttributePOptions = attributeOptionsBuilder.build();
      mFileSystem.createDirectory(turi,
          CreateDirectoryPOptions.newBuilder()
              .setMode(new alluxio.security.authorization.Mode((short) mode).toProto())
              .build());
      if (gid != GID || uid != UID) {
        LOG.debug("Set attributes of path {} to {}", path, setAttributePOptions);
        mFileSystem.setAttribute(turi, setAttributePOptions);
      }
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to create directory {}, directory already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Failed to create directory {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to create directory {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Opens an existing file for reading.
   *
   * Note that the opening an existing file would fail, because of Alluxio's write-once semantics.
   *
   * @param path the FS path of the file to open
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int open(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> openInternal(path, fi), "open", "path=%s", path);
  }

  private int openInternal(String path, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    // (see {@code man 2 open} for the structure of the flags bitfield)
    // File creation flags are the last two bits of flags
    final int flags = fi.flags.get();
    if (mOpenFiles.size() >= MAX_OPEN_FILES) {
      LOG.error("Cannot open {}: too many open files (MAX_OPEN_FILES: {})", path, MAX_OPEN_FILES);
      return ErrorCodes.EMFILE();
    }
    FileInStream is;
    try {
      try {
        is = mFileSystem.openFile(uri);
      } catch (FileIncompleteException e) {
        if (AlluxioFuseUtils.waitForFileCompleted(mFileSystem, uri)) {
          is = mFileSystem.openFile(uri);
        } else {
          throw e;
        }
      }
    } catch (OpenDirectoryException e) {
      LOG.error("Cannot open folder {}", path);
      return -ErrorCodes.EISDIR();
    } catch (FileIncompleteException e) {
      LOG.error("Cannot open incomplete file {}", path);
      return -ErrorCodes.EFAULT();
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.error("Failed to open file {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to open file {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    long fid = mNextOpenFileId.getAndIncrement();
    mOpenFiles.add(new OpenFileEntry<>(fid, path, is, null));
    fi.fh.set(fid);

    return 0;
  }

  /**
   * Reads data from an open file.
   *
   * @param path the FS path of the file to read
   * @param buf FUSE buffer to fill with data read
   * @param size how many bytes to read. The maximum value that is accepted
   *             on this method is {@link Integer#MAX_VALUE} (note that current
   *             FUSE implementation will call this method with a size of
   *             at most 128K).
   * @param offset offset of the read operation
   * @param fi FileInfo data structure kept by FUSE
   * @return the number of bytes read or 0 on EOF. A negative
   *         value on error
   */
  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readInternal(path, buf, size, offset, fi),
        "read", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int readInternal(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot read more than Integer.MAX_VALUE");
      return -ErrorCodes.EINVAL();
    }

    final long fd = fi.fh.get();
    OpenFileEntry<FileInStream, FileOutStream> oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (oe.getIn() == null) {
      LOG.error("{} was not open for reading", path);
      return -ErrorCodes.EBADFD();
    }
    int rd = 0;
    int nread = 0;
    synchronized (oe) {
      if (!mOpenFiles.contains(oe)) {
        LOG.error("Cannot find fd for {} in table", path);
        return -ErrorCodes.EBADFD();
      }
      FileInStream is = oe.getIn();
      try {
        if (offset - is.getPos() < is.remaining()) {
          is.seek(offset);
          final int sz = (int) size;
          final byte[] dest = new byte[sz];
          while (rd >= 0 && nread < sz) {
            rd = oe.getIn().read(dest, nread, sz - nread);
            if (rd >= 0) {
              nread += rd;
            }
          }

          if (nread == -1) { // EOF
            nread = 0;
          } else if (nread > 0) {
            buf.put(0, dest, 0, nread);
          }
        }
      } catch (Throwable t) {
        LOG.error("Failed to read file={}, offset={}, size={}", path, offset,
            size, t);
        return AlluxioFuseUtils.getErrorCode(t);
      }
    }
    return nread;
  }

  /**
   * Reads the contents of a directory.
   *
   * @param path The FS path of the directory
   * @param buff The FUSE buffer to fill
   * @param filter FUSE filter
   * @param offset Ignored in alluxio-fuse
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int readdir(String path, Pointer buff, FuseFillDir filter,
      @off_t long offset, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> readdirInternal(path, buff, filter, offset, fi),
        "readdir", "path=%s,buf=%s", path, buff);
  }

  private int readdirInternal(String path, Pointer buff, FuseFillDir filter,
      @off_t long offset, FuseFileInfo fi) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    try {
      final List<URIStatus> ls = mFileSystem.listStatus(turi);
      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (final URIStatus file : ls) {
        filter.apply(buff, file.getName(), null, 0);
      }
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to read directory {}, path does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to read directory {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Releases the resources associated to an open file. Release() is async.
   *
   * Guaranteed to be called once for each open() or create().
   *
   * @param path the FS path of the file to release
   * @param fi FileInfo data structure kept by FUSE
   * @return 0. The return value is ignored by FUSE (any error should be reported
   *         on flush instead)
   */
  @Override
  public int release(String path, FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> releaseInternal(path, fi), "release", "path=%s", path);
  }

  private int releaseInternal(String path, FuseFileInfo fi) {
    OpenFileEntry oe;
    final long fd = fi.fh.get();
    oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null || !mOpenFiles.remove(oe)) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    try {
      synchronized (oe) {
        oe.close();
      }
    } catch (IOException e) {
      LOG.error("Failed closing {} [in]", path, e);
    }
    return 0;
  }

  /**
   * Renames a path.
   *
   * @param oldPath the source path in the FS
   * @param newPath the destination path in the FS
   * @return 0 on success, a negative value on error
   */
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
      OpenFileEntry oe = mOpenFiles.getFirstByField(PATH_INDEX, oldPath);
      if (oe != null) {
        oe.setPath(newPath);
      }
    } catch (FileDoesNotExistException e) {
      LOG.debug("Failed to rename {} to {}, file {} does not exist", oldPath, newPath, oldPath);
      return -ErrorCodes.ENOENT();
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to rename {} to {}, file {} already exists", oldPath, newPath, newPath);
      return -ErrorCodes.EEXIST();
    } catch (Throwable t) {
      LOG.error("Failed to rename {} to {}", oldPath, newPath, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Deletes an empty directory.
   *
   * @param path The FS path of the directory
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rmdir(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "rmdir", "path=%s", path);
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
        "statfs", "path=%s", path);
  }

  private int statfsInternal(String path, Statvfs stbuf) {
    ClientContext ctx = ClientContext.create(sConf);

    try (BlockMasterClient blockClient =
             BlockMasterClient.Factory.create(MasterClientContext.newBuilder(ctx).build())) {
      Set<BlockMasterInfo.BlockMasterInfoField> blockMasterInfoFilter =
          new HashSet<>(Arrays.asList(
              BlockMasterInfo.BlockMasterInfoField.CAPACITY_BYTES,
              BlockMasterInfo.BlockMasterInfoField.FREE_BYTES,
              BlockMasterInfo.BlockMasterInfoField.USED_BYTES));
      BlockMasterInfo blockMasterInfo = blockClient.getBlockMasterInfo(blockMasterInfoFilter);

      // although user may set different block size for different files,
      // small block size can result more accurate compute.
      long blockSize = 4L * Constants.KB;
      // fs block size
      // The size in bytes of the minimum unit of allocation on this file system
      stbuf.f_bsize.set(blockSize);
      // The preferred length of I/O requests for files on this file system.
      stbuf.f_frsize.set(blockSize);
      // total data blocks in fs
      stbuf.f_blocks.set(blockMasterInfo.getCapacityBytes() / blockSize);
      // free blocks in fs
      long freeBlocks = blockMasterInfo.getFreeBytes() / blockSize;
      stbuf.f_bfree.set(freeBlocks);
      stbuf.f_bavail.set(freeBlocks);
      // inode info in fs
      // TODO(liuhongtong): support inode info
      stbuf.f_files.set(UNKNOWN_INODES);
      stbuf.f_ffree.set(UNKNOWN_INODES);
      stbuf.f_favail.set(UNKNOWN_INODES);
      // max file name length
      stbuf.f_namemax.set(MAX_NAME_LENGTH);
    } catch (IOException e) {
      LOG.error("statfs({}) failed:", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  /**
   * Changes the size of a file. This operation would not succeed because of Alluxio's write-once
   * model.
   */
  @Override
  public int truncate(String path, long size) {
    LOG.error("Truncate is not supported {}", path);
    return -ErrorCodes.EOPNOTSUPP();
  }

  /**
   * Deletes a file from the FS.
   *
   * @param path the FS path of the file
   * @return 0 on success, a negative value on error
   */
  @Override
  public int unlink(String path) {
    return AlluxioFuseUtils.call(LOG, () -> rmInternal(path), "unlink", "path=%s", path);
  }

  /**
   * Alluxio does not have access time, and the file is created only once. So this operation is a
   * no-op.
   */
  @Override
  public int utimens(String path, Timespec[] timespec) {
    return 0;
  }

  /**
   * Writes a buffer to an open Alluxio file. Random write is not supported, so the offset argument
   * is ignored. Also, due to an issue in OSXFUSE that may write the same content at a offset
   * multiple times, the write also checks that the subsequent write of the same offset is ignored.
   *
   * @param buf The buffer with source data
   * @param size How much data to write from the buffer. The maximum accepted size for writes is
   *        {@link Integer#MAX_VALUE}. Note that current FUSE implementation will anyway call write
   *        with at most 128K writes
   * @param offset The offset where to write in the file (IGNORED)
   * @param fi FileInfo data structure kept by FUSE
   * @return number of bytes written on success, a negative value on error
   */
  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset,
                   FuseFileInfo fi) {
    return AlluxioFuseUtils.call(LOG, () -> writeInternal(path, buf, size, offset, fi),
        "write", "path=%s,buf=%s,size=%d,offset=%d", path, buf, size, offset);
  }

  private int writeInternal(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (oe.getOut() == null) {
      LOG.error("{} already exists in Alluxio and cannot be overwritten."
          + " Please delete this file first.", path);
      return -ErrorCodes.EEXIST();
    }

    if (offset < oe.getWriteOffset()) {
      // no op
      return sz;
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(0, dest, 0, sz);
      oe.getOut().write(dest);
      oe.setWriteOffset(offset + size);
    } catch (IOException e) {
      LOG.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    }

    return sz;
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);

    try {
      mFileSystem.delete(turi);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to remove {}, file does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to remove {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Exposed for testing.
   */
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }

  @Override
  public void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts) {
    LOG.info(
        "Mount AlluxioFuseFileSystem: mountPoint=\"{}\", blocking={}, "
            + "debug={}, fuseOpts=\"{}\"",
        mountPoint, blocking, debug, Arrays.toString(fuseOpts));
    super.mount(mountPoint, blocking, debug, fuseOpts);
  }

  @Override
  public void umount(boolean force) {
    LOG.info("Umount AlluxioFuseFileSystem");
    super.umount();
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
