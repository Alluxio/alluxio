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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

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
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse.
 */
@ThreadSafe
public final class AlluxioFuseFileSystem extends FuseStubFS {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseFileSystem.class);
  private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
  private static final int MAX_OPEN_WAITTIME_MS = 5000;

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
  private final Map<Long, OpenFileEntry> mOpenFiles;

  private long mNextOpenFileId;

  /**
   * Creates a new instance of {@link AlluxioFuseFileSystem}.
   *
   * @param fs Alluxio file system
   * @param opts options
   */
  public AlluxioFuseFileSystem(FileSystem fs, AlluxioFuseOptions opts) {
    super();
    mFileSystem = fs;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mNextOpenFileId = 0L;
    mOpenFiles = new HashMap<>();

    final int maxCachedPaths = Configuration.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    mIsUserGroupTranslation
        = Configuration.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
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
    AlluxioURI uri = mPathResolverCache.getUnchecked(path);

    SetAttributeOptions options = SetAttributeOptions.defaults().setMode(new Mode((short) mode));
    try {
      mFileSystem.setAttribute(uri, options);
    } catch (IOException | AlluxioException e) {
      LOG.error("Exception on {} of changing mode to {}", path, mode, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  /**
   * Changes the user and group ownership of an Alluxio file.
   * This operation only works when the user group translation is enabled in Alluxio-Fuse.
   *
   * @param path the path of the file
   * @param uid the uid to change to
   * @param gid the gid to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chown(String path, @uid_t long uid, @gid_t long gid) {
    if (!mIsUserGroupTranslation) {
      LOG.info("Cannot change the owner of path {}. Please set {} to be true to enable "
          + "user group translation in Alluxio-Fuse.",
          path, PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED.getName());
      return -ErrorCodes.ENOSYS();
    }

    try {
      SetAttributeOptions options = SetAttributeOptions.defaults();
      final AlluxioURI uri = mPathResolverCache.getUnchecked(path);

      String userName = "";
      if (uid != ID_NOT_SET_VALUE && uid != ID_NOT_SET_VALUE_UNSIGNED) {
        userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EFAULT();
        }
        options.setOwner(userName);
      }

      String groupName = "";
      if (gid != ID_NOT_SET_VALUE && gid != ID_NOT_SET_VALUE_UNSIGNED) {
        groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get group name from gid {}", gid);
          return -ErrorCodes.EFAULT();
        }
        options.setGroup(groupName);
      } else if (!userName.isEmpty()) {
        groupName = AlluxioFuseUtils.getGroupName(userName);
        options.setGroup(groupName);
      }

      if (userName.isEmpty() && groupName.isEmpty()) {
        // This should never be reached
        LOG.info("Unable to change owner and group of file {} when uid is {} and gid is {}",
            path, userName, groupName);
      } else if (userName.isEmpty()) {
        LOG.info("Change group of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, options);
      } else {
        LOG.info("Change owner of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, options);
      }
    } catch (IOException | AlluxioException e) {
      LOG.error("Exception on {}", path, e);
      return -ErrorCodes.EIO();
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
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    final int flags = fi.flags.get();
    LOG.trace("create({}, {}) [Alluxio: {}]", path, Integer.toHexString(flags), uri);

    try {
      synchronized (mOpenFiles) {
        if (mOpenFiles.size() >= MAX_OPEN_FILES) {
          LOG.error("Cannot open {}: too many open files (MAX_OPEN_FILES: {})", uri,
              MAX_OPEN_FILES);
          return -ErrorCodes.EMFILE();
        }

        final OpenFileEntry ofe = new OpenFileEntry(null, mFileSystem.createFile(uri));
        LOG.debug("Alluxio OutStream created for {}", path);
        mOpenFiles.put(mNextOpenFileId, ofe);
        fi.fh.set(mNextOpenFileId);

        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mNextOpenFileId += 1;
      }
      LOG.debug("{} created and opened", path);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("File {} already exists", uri, e);
      return -ErrorCodes.EEXIST();
    } catch (IOException e) {
      LOG.error("IOException on  {}", uri, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException on {}", uri, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
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
    LOG.trace("flush({})", path);
    final long fd = fi.fh.get();
    OpenFileEntry oe;
    synchronized (mOpenFiles) {
      oe = mOpenFiles.get(fd);
    }
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }
    if (oe.getOut() != null) {
      try {
        oe.getOut().flush();
      } catch (IOException e) {
        LOG.error("IOException on  {}", path, e);
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
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("getattr({}) [Alluxio: {}]", path, turi);
    try {
      if (!mFileSystem.exists(turi)) {
        return -ErrorCodes.ENOENT();
      }
      final URIStatus status = mFileSystem.getStatus(turi);
      stat.st_size.set(status.getLength());

      final long ctime_sec = status.getLastModificationTimeMs() / 1000;
      //keeps only the "residual" nanoseconds not caputred in
      // citme_sec
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
    } catch (InvalidPathException e) {
      LOG.debug("Invalid path {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException on {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
    }

    return 0;
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFSName() {
    return Configuration.get(PropertyKey.FUSE_FS_NAME);
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
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("mkdir({}) [Alluxio: {}]", path, turi);
    try {
      mFileSystem.createDirectory(turi);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Cannot make dir. {} already exists", path, e);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Cannot make dir. Invalid path: {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("Cannot make dir. IOException: {}", path, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("Cannot make dir. {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
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
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    // (see {@code man 2 open} for the structure of the flags bitfield)
    // File creation flags are the last two bits of flags
    final int flags = fi.flags.get();
    LOG.trace("open({}, 0x{}) [Alluxio: {}]", path, Integer.toHexString(flags), uri);

    try {
      if (!mFileSystem.exists(uri)) {
        LOG.error("File {} does not exist", uri);
        return -ErrorCodes.ENOENT();
      }
      final URIStatus status = mFileSystem.getStatus(uri);
      if (status.isFolder()) {
        LOG.error("File {} is a directory", uri);
        return -ErrorCodes.EISDIR();
      }

      if (!status.isCompleted() && !waitForFileCompleted(uri)) {
        LOG.error("File {} has not completed", uri);
        return -ErrorCodes.EFAULT();
      }

      synchronized (mOpenFiles) {
        if (mOpenFiles.size() == MAX_OPEN_FILES) {
          LOG.error("Cannot open {}: too many open files", uri);
          return ErrorCodes.EMFILE();
        }
        final OpenFileEntry ofe = new OpenFileEntry(mFileSystem.openFile(uri), null);
        mOpenFiles.put(mNextOpenFileId, ofe);
        fi.fh.set(mNextOpenFileId);

        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mNextOpenFileId += 1;
      }
    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException on {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
    }

    return 0;
  }

  /**
   * Reads data from an open file.
   *
   * @param path the FS path of the file to read
   * @param buf FUSE buffer to fill with data read
   * @param size how many bytes to read. The maximum value that is accepted
   *             on this method is {@link Integer#MAX_VALUE} (note that current
   *             FUSE implementation will call this metod whit a size of
   *             at most 128K).
   * @param offset offset of the read operation
   * @param fi FileInfo data structure kept by FUSE
   * @return the number of bytes read or 0 on EOF. A negative
   *         value on error
   */
  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {

    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot read more than Integer.MAX_VALUE");
      return -ErrorCodes.EINVAL();
    }
    LOG.trace("read({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe;
    synchronized (mOpenFiles) {
      oe = mOpenFiles.get(fd);
    }
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    int rd = 0;
    int nread = 0;
    if (oe.getIn() == null) {
      LOG.error("{} was not open for reading", path);
      return -ErrorCodes.EBADFD();
    }
    try {
      oe.getIn().seek(offset);
      final byte[] dest = new byte[sz];
      while (rd >= 0 && nread < size) {
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
    } catch (IOException e) {
      LOG.error("IOException while reading from {}.", path, e);
      return -ErrorCodes.EIO();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
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
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("readdir({}) [Alluxio: {}]", path, turi);

    try {
      if (!mFileSystem.exists(turi)) {
        return -ErrorCodes.ENOENT();
      }
      final URIStatus status = mFileSystem.getStatus(turi);
      if (!status.isFolder()) {
        return -ErrorCodes.ENOTDIR();
      }
      final List<URIStatus> ls = mFileSystem.listStatus(turi);
      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (final URIStatus file : ls) {
        filter.apply(buff, file.getName(), null, 0);
      }
    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (InvalidPathException e) {
      LOG.debug("Invalid path {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException on {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
    }

    return 0;
  }

  /**
   * Releases the resources associated to an open file.
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
    LOG.trace("release({})", path);
    final long fd = fi.fh.get();
    OpenFileEntry oe;
    synchronized (mOpenFiles) {
      oe = mOpenFiles.remove(fd);
      if (oe == null) {
        LOG.error("Cannot find fd for {} in table", path);
        return -ErrorCodes.EBADFD();
      }
    }

    try {
      oe.close();
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
    final AlluxioURI oldUri = mPathResolverCache.getUnchecked(oldPath);
    final AlluxioURI newUri = mPathResolverCache.getUnchecked(newPath);
    LOG.trace("rename({}, {}) [Alluxio: {}, {}]", oldPath, newPath, oldUri, newUri);

    try {
      if (!mFileSystem.exists(oldUri)) {
        LOG.error("File {} does not exist", oldPath);
        return -ErrorCodes.ENOENT();
      }
      if (mFileSystem.exists(newUri)) {
        LOG.error("File {} already exists, please delete the destination file first", newPath);
        return -ErrorCodes.EEXIST();
      }
      mFileSystem.rename(oldUri, newUri);
    } catch (FileDoesNotExistException e) {
      LOG.debug("File {} does not exist", oldPath);
      return -ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException while moving {} to {}", oldPath, newPath, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("Exception while moving {} to {}", oldPath, newPath, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on mv {} {}", oldPath, newPath, e);
      return -ErrorCodes.EFAULT();
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
    LOG.trace("rmdir({})", path);
    return rmInternal(path, false);
  }

  /**
   * Changes the size of a file. This operation would not succeed because of Alluxio's write-once
   * model.
   */
  @Override
  public int truncate(String path, long size) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      if (!mFileSystem.exists(uri)) {
        LOG.error("File {} does not exist", uri);
        return -ErrorCodes.ENOENT();
      }
    } catch (IOException e) {
      LOG.error("IOException encountered at path {}", path, e);
      return -ErrorCodes.EIO();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException encountered at path {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception at path {}", path, e);
      return -ErrorCodes.EFAULT();
    }
    LOG.error("File {} exists and cannot be overwritten. Please delete the file first", uri);
    return -ErrorCodes.EEXIST();
  }

  /**
   * Deletes a file from the FS.
   *
   * @param path the FS path of the file
   * @return 0 on success, a negative value on error
   */
  @Override
  public int unlink(String path) {
    LOG.trace("unlink({})", path);
    return rmInternal(path, true);
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
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    LOG.trace("write({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe;
    synchronized (mOpenFiles) {
      oe = mOpenFiles.get(fd);
    }
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
   * Convenience internal method to remove files or directories.
   *
   * @param path The path to remove
   * @param mustBeFile When true, returns an error when trying to
   *                   remove a directory
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path, boolean mustBeFile) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);

    try {
      if (!mFileSystem.exists(turi)) {
        LOG.error("File {} does not exist", turi);
        return -ErrorCodes.ENOENT();
      }
      final URIStatus status = mFileSystem.getStatus(turi);
      if (mustBeFile && status.isFolder()) {
        LOG.error("File {} is a directory", turi);
        return -ErrorCodes.EISDIR();
      }

      mFileSystem.delete(turi);
    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      return -ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      return -ErrorCodes.EIO();
    } catch (DirectoryNotEmptyException e) {
      LOG.error("{} is not empty", path, e);
      return -ErrorCodes.ENOTEMPTY();
    } catch (AlluxioException e) {
      LOG.error("AlluxioException on {}", path, e);
      return -ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
    }

    return 0;
  }

  /**
   * Waits for the file to complete before opening it.
   *
   * @param uri the file path to check
   * @return whether the file is completed or not
   */
  private boolean waitForFileCompleted(AlluxioURI uri) {
    try {
      CommonUtils.waitFor("file completed", () -> {
        try {
          return mFileSystem.getStatus(uri).isCompleted();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, WaitForOptions.defaults().setTimeoutMs(MAX_OPEN_WAITTIME_MS));
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException te) {
      return false;
    }
  }

  /**
   * Exposed for testing.
   */
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
