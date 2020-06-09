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
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.FuseFillDir;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.resource.LockResource;
import alluxio.util.ThreadUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;
  private final AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final String mFsName;

  private static final int LOCK_SIZE = 2048;
  /** A readwrite lock pool to guard individual files based on striping. */
  private final ReadWriteLock[] mFileLocks = new ReentrantReadWriteLock[LOCK_SIZE];

  private final Map<Long, OpenFileEntry> mOpenFiles = new ConcurrentHashMap<>();

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
      FileSystem fs, AlluxioFuseOptions opts, AlluxioConfiguration conf) {
    super(Paths.get(opts.getMountPoint()));
    mFsName = conf.get(PropertyKey.FUSE_FS_NAME);
    mFileSystem = fs;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mPathResolverCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX))
        .build(new PathCacheLoader());
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

  @Override
  public int getattr(String path, FileStat stat) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    try {
      URIStatus status = mFileSystem.getStatus(turi);
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

      int mode = status.getMode();
      if (status.isFolder()) {
        mode |= FileStat.S_IFDIR;
      } else {
        mode |= FileStat.S_IFREG;
      }
      stat.st_mode.set(mode);
      stat.st_nlink.set(1);
    } catch (Throwable e) {
      LOG.error("Failed to getattr {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int readdir(String path, long buff, FuseFillDir filter, long offset,
                     FuseFileInfo fi) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    try {
      final List<URIStatus> ls = mFileSystem.listStatus(turi);
      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (final URIStatus file : ls) {
        filter.apply(buff, file.getName(), null, 0);
      }
    } catch (Throwable e) {
      LOG.error("Failed to readdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }

    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    try {
      long fd = mNextOpenFileId.getAndIncrement();
      FileInStream is = mFileSystem.openFile(uri);
      mOpenFiles.put(fd, new OpenFileEntry(path, is));
      fi.fh.set(fd);
      LOG.info("open(fd={},entries={})", fd, mOpenFiles.size());
      ((BaseFileSystem) mFileSystem).getFileSystemContext().printAvailableBlockWorkerClient();
      return 0;
    } catch (Throwable e) {
      LOG.error("Failed to open {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    int nread = 0;
    int rd = 0;
    final int sz = (int) size;
    long fd = fi.fh.get();
    // FileInStream is not thread safe
    try (LockResource r1 = new LockResource(getFileLock(fd).writeLock())) {
      OpenFileEntry oe = mOpenFiles.get(fd);
      if (oe == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      oe.getIn().seek(offset);
      FileInStream is = oe.getIn();
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
  public int flush(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    final OpenFileEntry oe;
    long fd = fi.fh.get();
    LOG.info("release(fd={},entries={})", fd, mOpenFiles.size());
    ((BaseFileSystem) mFileSystem).getFileSystemContext().printAvailableBlockWorkerClient();
    try (LockResource r1 = new LockResource(getFileLock(fd).writeLock())) {
      oe = mOpenFiles.remove(fd);
      if (oe == null) {
        LOG.error("Cannot find fd {} for {}", fd, path);
        return -ErrorCodes.EBADFD();
      }
      oe.close();
    } catch (Throwable e) {
      LOG.error("Failed closing {}", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
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

  @ThreadSafe
  private final class OpenFileEntry implements Closeable {
    private final FileInStream mIn;

    // Path is likely to be changed when fuse rename() is called
    private String mPath;

    /** the ref count.  */
    private final AtomicInteger mCount;

    private final Closer mCloser;

    /**
     * Constructs a new {@link alluxio.fuse.OpenFileEntry} for an Alluxio file.
     *
     * @param path the path of the file
     * @param in the input stream of the file
     */
    public OpenFileEntry(String path, FileInStream in) {
      Preconditions.checkNotNull(in, "in");
      mIn = in;
      mPath = path;
      mCount = new AtomicInteger(1);
      mCloser = Closer.create();
      mCloser.register(mIn);
    }

    /**
     * @return the path of the file
     */
    public String getPath() {
      return mPath;
    }

    /**
     * Gets the opened input stream for this open file entry. The value returned can be {@code null}
     * if the file is not open for reading.
     *
     * @return an opened input stream for the open alluxio file, or null
     */
    public FileInStream getIn() {
      return mIn;
    }

    /**
     * @return the ref count
     */
    public AtomicInteger getCount() {
      return mCount;
    }

    @Override
    public void close() throws IOException {
      mCloser.close();
    }
  }
}
