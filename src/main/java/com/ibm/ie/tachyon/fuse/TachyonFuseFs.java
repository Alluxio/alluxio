/*
 * Licensed to IBM Ireland - Research and Development under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.ie.tachyon.fuse;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import static jnr.constants.platform.OpenFlags.O_RDONLY;
import static jnr.constants.platform.OpenFlags.O_WRONLY;


/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse
 * @author Andrea Reale <realean2@ie.ibm.com>
 */
public final class TachyonFuseFs extends FuseStubFS {
  private final static int MAX_OPEN_FILES = Integer.MAX_VALUE;
  // Limits the number of translated (FUSE to TachyonURI)  paths that are kept
  // in memory
  private final static int MAX_CACHED_PATHS = 500;
  private final static Logger LOG = LoggerFactory.getLogger(TachyonFuseFs.class);
  private final static long[] mUidAndGid = TachyonFuseUtils.getUidAndGid();


  private final TachyonFileSystem mTFS;
  private final Path mMountPoint;
  private final Path mTachyonRootPath;
  private final String mTachyonMaster;
  // Keeps a cache of the most recently translated paths from String to TachyonURI
  private final LoadingCache<String, TachyonURI> mPathResolverCache;

  private final Object mOpenFilesLock;
  // Table of open files with corresponding InputStreams and OutptuStreams
  private final Map<Long, OpenFileEntry> mOpenFiles;
  private long mOpenFileIds;


  public TachyonFuseFs(TachyonFuseOptions opts) {
    super();
    mTFS = TachyonFileSystem.TachyonFileSystemFactory.get();
    mMountPoint = Paths.get(opts.getMountPoint());
    mTachyonMaster = opts.getMasterAddress();
    mTachyonRootPath = Paths.get(opts.getTachyonRoot());
    mOpenFileIds = 0L;
    mOpenFiles = new HashMap<>();
    mOpenFilesLock = new Object();

    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(MAX_CACHED_PATHS)
        .build(new PathCacheLoader());

    Preconditions.checkArgument(mMountPoint.isAbsolute(),
        "mount point should be an absolute path");
    Preconditions.checkArgument(mTachyonRootPath.isAbsolute(),
        "tachyon root path should be absolute");
  }

  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    int ret = 0;
    final TachyonURI turi = mPathResolverCache.getUnchecked(path);
    final int flags = fi.flags.get();
    LOG.trace("create({}, {}) [Tachyon: {}]", path, Integer.toHexString(flags), turi);
    LOG.warn("{}: mode is ignored in tachyon-fuse", path);
    if ((flags & 3) != O_WRONLY.intValue()) {
      LOG.error("Files can only be created in O_WRONLY mode ({})", path);
      return -ErrorCodes.EACCES();
    }

    try {

      final OpenFileEntry ofe = new OpenFileEntry();
      ofe.in = Optional.empty();
      ofe.out = Optional.of(mTFS.getOutStream(turi));
      LOG.debug("Tachyon OutStream created for {}", path);

      synchronized (mOpenFilesLock) {
        if (mOpenFiles.size() == MAX_OPEN_FILES) {
          LOG.error("Cannot open {}: too many open files", turi);
          return ErrorCodes.EMFILE();
        }
        mOpenFiles.put(mOpenFileIds, ofe);
        fi.fh.set(mOpenFileIds);

        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mOpenFileIds += 1;
      }
      LOG.debug("{} created and opened in O_WRONLY mode", path);

    } catch (FileAlreadyExistsException e) {
      LOG.debug("File {} already exists", turi, e);
      ret = ErrorCodes.EEXIST();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (TachyonException e) {
      LOG.error("TachyonException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      ret = ErrorCodes.EFAULT();
    }

    return -ret;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    int ret = 0;
    final TachyonURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("getattr({}) [Tachyon: {}]", path, turi);
    try {
      final TachyonFile tf = mTFS.openIfExists(turi);
      if (tf == null) {
        ret = ErrorCodes.ENOENT();
      } else {
        final FileInfo fi = mTFS.getInfo(tf);
        stat.st_size.set(fi.getLength());

        final long ctime = fi.getLastModificationTimeMs();
        final long ctime_sec = fi.getLastModificationTimeMs() / 1000;
        final long ctime_nsec = (fi.getLastModificationTimeMs() % 1000) * 1000;
        stat.st_ctim.tv_sec.set(ctime_sec);
        stat.st_ctim.tv_nsec.set(ctime_nsec);
        stat.st_mtim.tv_sec.set(ctime_sec);
        stat.st_mtim.tv_nsec.set(ctime_nsec);

        System.getProperty("user.name");

        stat.st_uid.set(mUidAndGid[0]);
        stat.st_gid.set(mUidAndGid[1]);

        final int mode;
        if (fi.isFolder) {
          mode = FileStat.S_IFDIR;
        } else {
          mode = FileStat.S_IFREG;
        }
        stat.st_mode.set(mode);
      }

    } catch (InvalidPathException e) {
      LOG.debug("Invalid path {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (TachyonException e) {
      LOG.error("TachyonException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      ret = ErrorCodes.EFAULT();
    }

    return -ret;
  }

  @Override
  public String getFSName() {
    return "tachyon-fuse";
  }

  @Override
  public int mkdir(String path, @mode_t long mode) {
    int ret = 0;
    final TachyonURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("mkdir({}) [Tachyon: {}]", path, turi);
    LOG.warn("{}: mode is ignored in tachyon-fuse", path);
    try {
      mTFS.mkdir(turi);
    } catch(FileAlreadyExistsException e) {
      LOG.debug("Cannot make dir. {} already exists", path, e);
      ret = ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Cannot make dir. Invalid path: {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("Cannot make dir. IOException: {}", path, e);
      ret = ErrorCodes.EIO();
    } catch (TachyonException e) {
      LOG.error("Cannot make dir. {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      ret = ErrorCodes.EFAULT();
    }

    return -ret;

  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    int ret = 0;
    final TachyonURI turi = mPathResolverCache.getUnchecked(path);
    final int flags = fi.flags.get();
    LOG.trace("open({}, 0x{}) [Tachyon: {}]", path, Integer.toHexString(flags), turi);
    LOG.warn("{}: mode is ignored in tachyon-fuse", path);

    if ((flags & 3) != O_RDONLY.intValue()) {
      LOG.error("Files can only be opened in O_RDONLY mode ({})", path);
      return -ErrorCodes.EACCES();
    }
    try {
      final TachyonFile tf = mTFS.openIfExists(turi);
      if (tf == null) {
        LOG.error("File {} does not exist", turi);
        return -ErrorCodes.ENOENT();
      }
      final FileInfo tfi = mTFS.getInfo(tf);
      if (tfi.isFolder) {
        LOG.error("File {} is a directory", turi);
        return -ErrorCodes.ENOENT();
      }
      final OpenFileEntry ofe = new OpenFileEntry();
      ofe.tfid = tf.getFileId();
      ofe.in = Optional.of(mTFS.getInStream(tf));
      ofe.out = Optional.empty();

      synchronized (mOpenFilesLock) {
        if (mOpenFiles.size() == MAX_OPEN_FILES) {
          LOG.error("Cannot open {}: too many open files", turi);
          return ErrorCodes.EMFILE();
        }
        mOpenFiles.put(mOpenFileIds, ofe);
        fi.fh.set(mOpenFileIds);

        // Assuming I will never wrap around (2^64 open files are quite a lot anyway)
        mOpenFileIds += 1;
      }


    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      ret = ErrorCodes.EIO();
    } catch (TachyonException e) {
      LOG.error("TachyonException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      ret = ErrorCodes.EFAULT();
    }


    return -ret;
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
                  FuseFileInfo fi) {

    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot read more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    LOG.trace("read({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe = null;
    synchronized (mOpenFilesLock) {
      oe = mOpenFiles.get(fd);
    }
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    int rd = 0, nread = 0;
    if (!oe.in.isPresent()) {
      LOG.error("{} was not open for reading", path);
      return -ErrorCodes.EBADFD();
    }
    final FileInStream instream = oe.in.get();
    try {
      instream.seek(offset);

      final byte[] dest = new byte[sz];
      while (rd >= 0 && nread < size) {
        rd = instream.read(dest, nread, sz - nread);
        if (rd >= 0) {
          nread += rd;
        }
      }

      buf.put(0, dest, 0, nread);
    } catch (IOException e) {
      LOG.error("IOException while reading from {}.", path, e);
      return -ErrorCodes.EIO();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      return -ErrorCodes.EFAULT();
    }

    return nread;

  }

  @Override
  public int readdir(String path, Pointer buff, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    int ret = 0;
    final TachyonURI turi = mPathResolverCache.getUnchecked(path);
    LOG.trace("readdir({}) [Tachyon: {}]", path, turi);

    try {
      final TachyonFile tf = mTFS.openIfExists(turi);
      if (tf == null) {
        return -ErrorCodes.ENOTDIR();
      }
      final FileInfo tfi = mTFS.getInfo(tf);
      if (!tfi.isFolder) {
        return -ErrorCodes.ENOTDIR();
      }
      final List<FileInfo> ls = mTFS.listStatus(tf);
      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (final FileInfo file : ls) {
        filter.apply(buff, file.name, null, 0);
      }

    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (InvalidPathException e) {
      LOG.debug("Invalid path {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (TachyonException e) {
      LOG.error("TachyonException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      ret = ErrorCodes.EFAULT();
    }

    return -ret;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    LOG.trace("release({})", path);
    final long fd = fi.fh.get();
    OpenFileEntry oe = null;
    synchronized (mOpenFilesLock) {
      oe = mOpenFiles.remove(fd);
      if (oe == null) {
        LOG.error("Cannot find fd for {} in table", path);
        return -ErrorCodes.EBADFD();
      }
    }

    oe.in.ifPresent((FileInStream is) -> {
      try {
        is.close();
      } catch (IOException e) {
        LOG.error("Failed closing {} [in]", path, e);
      }
    });

    oe.out.ifPresent((FileOutStream os) -> {
      try {
        LOG.trace("Closing file writer for {}", path);
        os.close();
      } catch (IOException e) {
        LOG.error("Failed closing {} [out]", path, e);
      }
    });
    return 0;
  }

  @Override
  public int rename(String oldPath, String newPath) {
    int ret = 0;
    final TachyonURI oldUri = mPathResolverCache.getUnchecked(oldPath);
    final TachyonURI newUri = mPathResolverCache.getUnchecked(newPath);
    LOG.trace("rename({}, {}) [Tachyon: {}, {}]", oldPath, newPath, oldUri, newUri);

    try {
      final TachyonFile oldFile = mTFS.openIfExists(oldUri);
      if (oldFile == null) {
        LOG.error("File {} does not exist", oldPath);
        ret = ErrorCodes.ENOENT();
      } else {
        mTFS.rename(oldFile, newUri);
      }
    } catch (FileDoesNotExistException e) {
      LOG.debug("File {} does not exist", oldPath);
      ret = ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException while moving {} to {}", oldPath, newPath, e);
      ret = ErrorCodes.EIO();
    } catch (TachyonException e) {
      LOG.error("Exception while moving {} to {}", oldPath, newPath, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on mv {} {}", oldPath, newPath, e);
      ret = ErrorCodes.EFAULT();
    }

    return -ret;
  }

  @Override
  public int rmdir(String path) {
    LOG.trace("rmdir({})", path);
    return rmInternal(path, false);
  }


  @Override
  public int unlink(String path) {
    LOG.trace("unlink({})", path);
    return rmInternal(path, true);
  }

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
    OpenFileEntry oe = null;
    synchronized (mOpenFilesLock) {
      oe = mOpenFiles.get(fd);
    }
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (!oe.out.isPresent()) {
      LOG.error("{} was not open for writing", path);
      return -ErrorCodes.EBADFD();
    }
    final FileOutStream outstream = oe.out.get();
    // LOG.debug("Tachyn-fuse does not support seek for writes. Ignoring offset {}", offset);
    try {

      final byte[] dest = new byte[sz];
      buf.get(0, dest, 0, sz);
      outstream.write(dest);
    } catch (IOException e) {
      LOG.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    }

    return sz;
  }

  private int rmInternal(String path, boolean mustBeFile) {
    int ret = 0;
    final TachyonURI turi = mPathResolverCache.getUnchecked(path);

    try {
      final TachyonFile tf = mTFS.openIfExists(turi);
      if (tf == null) {
        LOG.error("File {} does not exist", turi);
        return -ErrorCodes.ENOENT();
      }
      final FileInfo tfi = mTFS.getInfo(tf);
      if (mustBeFile && tfi.isFolder) {
        LOG.error("File {} is a directory", turi);
        return -ErrorCodes.EISDIR();
      }

      mTFS.delete(tf);
    } catch (FileDoesNotExistException e) {
      LOG.debug("File does not exist {}", path, e);
      ret = ErrorCodes.ENOENT();
    } catch (IOException e) {
      LOG.error("IOException on {}", path, e);
      ret = ErrorCodes.EIO();
    } catch (TachyonException e) {
      LOG.error("TachyonException on {}", path, e);
      ret = ErrorCodes.EFAULT();
    } catch (Throwable e) {
      LOG.error("Unexpected exception on {}", path, e);
      ret = ErrorCodes.EFAULT();
    }

    return -ret;

  }

  /**
   * Resolves a FUSE path into a TachyonURI and possibly keeps it int cache
   */
  private class PathCacheLoader extends CacheLoader<String, TachyonURI> {
    @Override
    public TachyonURI load(String fusePath) {
      final Path p = Paths.get(fusePath);
      final Path tpath = mTachyonRootPath.resolve(p);

      return new TachyonURI(mTachyonMaster + tpath.toString());
    }
  }
}
