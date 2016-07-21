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

import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_WRONLY;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse.
 */
@ThreadSafe
final class AlluxioFuseFileSystem extends FuseStubFS {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
  private static final long[] UID_AND_GID = AlluxioFuseUtils.getUidAndGid();

  private final FileSystem mFileSystem;
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  private final String mAlluxioMaster;
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
  AlluxioFuseFileSystem(FileSystem fs, AlluxioFuseOptions opts) {
    super();
    mFileSystem = fs;
    mAlluxioMaster = Configuration.get(PropertyKey.MASTER_ADDRESS);
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mNextOpenFileId = 0L;
    mOpenFiles = new HashMap<>();

    final int maxCachedPaths = Configuration.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(maxCachedPaths)
        .build(new PathCacheLoader());

    Preconditions.checkArgument(mAlluxioRootPath.isAbsolute(),
        "alluxio root path should be absolute");
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
    // mode is ignored in alluxio-fuse
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    // (see {@code man 2 open} for the structure of the flags bitfield)
    // File creation flags are the last two bits of flags
    final int flags = fi.flags.get();
    LOG.trace("create({}, {}) [Alluxio: {}]", path, Integer.toHexString(flags), turi);
    final int openFlag = flags & 3;
    if (openFlag != O_WRONLY.intValue()) {
      OpenFlags flag = OpenFlags.valueOf(openFlag);
      LOG.error("Passed a {} flag to create(). Files can only be created in O_WRONLY mode ({})",
          flag.toString(), path);
      return -ErrorCodes.EACCES();
    }

    try {
      synchronized (mOpenFiles) {
        if (mOpenFiles.size() >= MAX_OPEN_FILES) {
          LOG.error("Cannot open {}: too many open files (MAX_OPEN_FILES: {})",
              turi, MAX_OPEN_FILES);
          return -ErrorCodes.EMFILE();
        }

        final OpenFileEntry ofe = new OpenFileEntry(null, mFileSystem.createFile(turi));
        LOG.debug("Alluxio OutStream created for {}", path);
        mOpenFiles.put(mNextOpenFileId, ofe);
        fi.fh.set(mNextOpenFileId);

        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mNextOpenFileId += 1;
      }
      LOG.debug("{} created and opened in O_WRONLY mode", path);

    } catch (FileAlreadyExistsException e) {
      LOG.debug("File {} already exists", turi, e);
      return -ErrorCodes.EEXIST();
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

      final long ctime = status.getLastModificationTimeMs();
      final long ctime_sec = status.getLastModificationTimeMs() / 1000;
      //keeps only the "residual" nanoseconds not caputred in
      // citme_sec
      final long ctime_nsec = (status.getLastModificationTimeMs() % 1000) * 1000;
      stat.st_ctim.tv_sec.set(ctime_sec);
      stat.st_ctim.tv_nsec.set(ctime_nsec);
      stat.st_mtim.tv_sec.set(ctime_sec);
      stat.st_mtim.tv_nsec.set(ctime_nsec);

      // TODO(andreareale): understand how to map FileInfo#getOwner()
      // and FileInfo#getGroup() to UIDs and GIDs of the node
      // where alluxio-fuse is mounted.
      // While this is not done, just use uid and gid of the user
      // running alluxio-fuse.
      stat.st_uid.set(UID_AND_GID[0]);
      stat.st_gid.set(UID_AND_GID[1]);

      final int mode;
      if (status.isFolder()) {
        mode = FileStat.S_IFDIR;
      } else {
        mode = FileStat.S_IFREG;
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
   * Note that the open mode <i>must</i> be
   * O_RDONLY, otherwise the open will fail. This is due to
   * the Alluxio "write-once/read-many-times" file model.
   *
   * @param path the FS path of the file to open
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int open(String path, FuseFileInfo fi) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    // (see {@code man 2 open} for the structure of the flags bitfield)
    // File creation flags are the last two bits of flags
    final int flags = fi.flags.get();
    LOG.trace("open({}, 0x{}) [Alluxio: {}]", path, Integer.toHexString(flags), turi);

    if ((flags & 3) != O_RDONLY.intValue()) {
      LOG.error("Files can only be opened in O_RDONLY mode ({})", path);
      return -ErrorCodes.EACCES();
    }
    try {
      if (!mFileSystem.exists(turi)) {
        LOG.error("File {} does not exist", turi);
        return -ErrorCodes.ENOENT();
      }
      final URIStatus status = mFileSystem.getStatus(turi);
      if (status.isFolder()) {
        LOG.error("File {} is a directory", turi);
        return -ErrorCodes.EISDIR();
      }

      synchronized (mOpenFiles) {
        if (mOpenFiles.size() == MAX_OPEN_FILES) {
          LOG.error("Cannot open {}: too many open files", turi);
          return ErrorCodes.EMFILE();
        }
        final OpenFileEntry ofe = new OpenFileEntry(mFileSystem.openFile(turi), null);
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
      } else {
        mFileSystem.rename(oldUri, newUri);
      }
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
   * Writes a buffer to an open Alluxio file.
   *
   * @param buf The buffer with source data
   * @param size How much data to write from the buffer. The maximum accepted size
   *             for writes is {@link Integer#MAX_VALUE}. Note that current FUSE
   *             implementation will anyway call write with at most 128K writes
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
      LOG.error("{} was not open for writing", path);
      return -ErrorCodes.EBADFD();
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(0, dest, 0, sz);
      oe.getOut().write(dest);
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
   * Exposed for testing.
   */
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }

  /**
   * Resolves a FUSE path into {@link AlluxioURI} and possibly keeps it in the cache.
   */
  private class PathCacheLoader extends CacheLoader<String, AlluxioURI> {

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

      return new AlluxioURI(mAlluxioMaster + tpath.toString());
    }
  }

}
