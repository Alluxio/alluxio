/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.orangefs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystem;

import org.orangefs.usrint.Orange;
import org.orangefs.usrint.OrangeFileSystemInputStream;
import org.orangefs.usrint.OrangeFileSystemLayout;
import org.orangefs.usrint.OrangeFileSystemOutputStream;
import org.orangefs.usrint.PVFS2POSIXJNIFlags;
import org.orangefs.usrint.PVFS2STDIOJNIFlags;
import org.orangefs.usrint.Stat;
import org.orangefs.usrint.Statfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * OrangeFS {@link UnderFileSystem} implementation using OrangeFS JNI shim and Direct Interface.
 */
@ThreadSafe
public class OrangeFSUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // The following flags are defined for the st_mode field:
  public static final int S_IFIFO = 0010000; // named pipe (fifo)
  public static final int S_IFCHR = 0020000; // character special
  public static final int S_IFDIR = 0040000; // directory
  public static final int S_IFBLK = 0060000; // block special
  public static final int S_IFREG = 0100000; // regular
  public static final int S_IFLNK = 0120000; // symbolic link
  public static final int S_IFSOCK = 0140000; // socket
  public static final int S_IFMT = 0170000; // file mask for type checks
  public static final int S_ISUID = 0004000; // set user id on execution
  public static final int S_ISGID = 0002000; // set group id on execution
  public static final int S_ISVTX = 0001000; // save swapped text even after use
  public static final int S_IRUSR = 0000400; // read permission, owner
  public static final int S_IWUSR = 0000200; // write permission, owner
  public static final int S_IXUSR = 0000100; // execute/search permission, owner
  public static final int S_IRGRP = 0000040; // read permission, group
  public static final int S_IWGRP = 0000020; // write permission, group
  public static final int S_IXGRP = 0000010; // execute/search permission, group
  public static final int S_IROTH = 0000004; // read permission, other
  public static final int S_IWOTH = 0000002; // write permission, other
  public static final int S_IXOTH = 0000001; // execute permission, other

  private static final int MAX_TRY = 5;
  private static final long DEFAULT_OFS_PERMISSION = 0700;
  public PVFS2POSIXJNIFlags mPosixFlags;
  public PVFS2STDIOJNIFlags mStdioFlags;
  private Orange mOrange;
  private int mOfsBufferSize;
  private int mOfsBlockSize;
  private OrangeFileSystemLayout mOfsLayout;
  private AlluxioURI mUri;
  private String mOfsMount;

  /**
   * Constructs a new instance of {@link OrangeFSUnderFileSystem}.
   *
   * @param path the name of the bucket
   * @param conf the configuration for Alluxio
   * @throws IOException when a connection to OrangeFS could not be created
   */
  public OrangeFSUnderFileSystem(String path, Configuration conf) throws IOException {
    super(conf);

    mOrange = Orange.getInstance();
    mPosixFlags = mOrange.posix.f;
    mStdioFlags = mOrange.stdio.f;
    int ordinal;
    boolean uriAuthorityMatchFound = false;

    mUri = new AlluxioURI(path);
    mOfsBufferSize = (int) conf.getBytes(Constants.UNDERFS_OFS_BUFFER_SIZE);
    mOfsBlockSize = (int) conf.getBytes(Constants.UNDERFS_OFS_BLOCK_SIZE);
    mOfsLayout = conf.getEnum(Constants.UNDERFS_OFS_LAYOUT, OrangeFileSystemLayout.class);

    String uriAuthority = mUri.getAuthority();
    List<String> ofsSystems = conf.getList(Constants.UNDERFS_OFS_SYSTEMS, ",");
    List<String> ofsMounts = conf.getList(Constants.UNDERFS_OFS_MNTLOCATIONS, ",");

    if (ofsSystems.isEmpty() || ofsMounts.isEmpty()) {
      throw new IOException("Configuration value alluxio.underfs.ofs.systems or"
          + "alluxio.underfs.ofs.mntLocations is null. These configuration values must be"
          + "defined and have at least one entry each.");
    }

    LOG.debug("Number of specified OrangeFS systems: " + ofsSystems.size());
    LOG.debug("Number of specified OrangeFS mount locations: " + ofsMounts.size());

    if (ofsSystems.size() != ofsMounts.size()) {
      throw new IOException(
          "Configuration values alluxio.underfs.ofs.systems and alluxio.underfs.ofs.mntLocations"
              + " must contain the same number of comma-separated elements.");
    }

    LOG.debug("Determining file system associated with URI authority:");
    for (ordinal = 0; ordinal < ofsSystems.size(); ordinal++) {
      LOG.debug("(ofsSystems[{}], ofsMounts[{}]) = ({}, {})", ordinal, ofsSystems.get(ordinal),
          ofsMounts.get(ordinal));
      if (uriAuthority.equals(ofsSystems.get(ordinal))) {
        LOG.debug("Match found. Continuing with OrangeFS initialization.");
        uriAuthorityMatchFound = true;
        break;
      }
    }

    if (uriAuthorityMatchFound) {
      mOfsMount = ofsMounts.get(ordinal);
      LOG.debug("Matching URI authority found at index = {}", ordinal);
    } else {
      LOG.error("No OrangeFS file system found matching the following authority: {}", uriAuthority);
      throw new IOException("There was no matching authority found in alluxio.underfs.ofs.systems."
          + "Check your configuration." + uriAuthority + " != " + ofsSystems.get(ordinal - 1));
    }

    LOG.debug("URI: " + this.mUri.toString());
    LOG.debug("URI Authority: " + uriAuthority);
    LOG.debug("Conf: " + conf.toString());
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.OFS;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
  }

  @Override
  public void connectFromMaster(Configuration conf, String hostname) {
    LOG.debug("connect from master");
  }

  @Override
  public void connectFromWorker(Configuration conf, String hostname) {
    LOG.debug("connect from worker");
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return create(path, mOfsBlockSize);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path, (short) 1, blockSizeByte);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        LOG.debug("Creating OrangeFS file at {}", path);
        return new OrangeFileSystemOutputStream(getOFSPath(path), mOfsBufferSize, replication,
            blockSizeByte, false, mOfsLayout);
      } catch (IOException e) {
        LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    boolean ret;
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    String ofsPath = getOFSPath(path);

    while (retryPolicy.attemptRetry()) {
      try {
        if (isFolder(path)) {
          if (!recursive) {
            LOG.debug("Couldn't delete Path f = {} since it is a directory but recursive is false.",
                ofsPath);
            return false;
          }
          // Call recursive delete on path
          LOG.debug(
              "Path f = {} is a directory and recursive is true. Recursively deleting directory.",
              ofsPath);
          ret = mOrange.stdio.recursiveDeleteDir(ofsPath) == 0;
        } else {
          LOG.debug("Path f = {} exists and is a regular file. unlinking.", getOFSPath(path));
          ret = mOrange.posix.unlink(ofsPath) == 0;
        }
        // Return false on failure.
        if (!ret) {
          LOG.debug("remove failed: ret == false");
        }
        return ret;
      } catch (IOException e) {
        LOG.error("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean exists(String path) throws IOException {
    return exists(new AlluxioURI(path));
  }

  /**
   * Checks if a file or folder exists in under file system.
   *
   * @param uri The file name with {@link AlluxioURI} form
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  public boolean exists(AlluxioURI uri) throws IOException {
    return getFileStatus(uri) != null;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return getFileStatus(path).st_blksize;
  }

  // Not supported
  @Override
  public Object getConf() {
    LOG.warn("getConf is not supported when using OrangeFSUnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public void setConf(Object conf) {}

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.warn("getFileLocations is not supported when using OrangeFSUnderFileSystem,"
        + "returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.warn(
        "getFileLocations is not supported when using OrangeFSUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    return getFileStatus(path).st_size;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    return getFileStatus(path).st_mtime * 1000;
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    Statfs statfs = getFileSystemStatus(path);

    if (statfs != null) {
      switch (type) {
        case SPACE_TOTAL:
          return statfs.getCapacity();
        case SPACE_USED:
          return statfs.getUsed();
        case SPACE_FREE:
          return statfs.getRemaining();
        default:
          throw new IOException("Unknown getSpace parameter: " + type);
      }
    }
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    Stat stats = getFileStatus(path);
    return (stats.st_mode & S_IFREG) != 0;
  }

  @Override
  public String[] list(String path) throws IOException {
    // if the path not exists, or it is a file, then should return null
    if (!exists(path) || !isFolder(path)) {
      return null;
    }
    return mOrange.stdio.getEntriesInDir(getOFSPath(path)).toArray(new String[0]);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    AlluxioURI uri;
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);

    while (retryPolicy.attemptRetry()) {
      try {
        if (exists(path)) {
          LOG.debug("Trying to create existing directory at {}", path);
          return false;
        }
        // Create directories one by one with explicit permissions.
        uri = new AlluxioURI(path);
        Stack<AlluxioURI> dirsToMake = new Stack<AlluxioURI>();
        dirsToMake.push(uri);
        AlluxioURI parent = uri.getParent();
        while (!exists(parent)) {
          dirsToMake.push(parent);
          parent = parent.getParent();
        }
        while (!dirsToMake.empty()) {
          String ofsDir = getOFSPath(dirsToMake.pop());
          if (mOrange.posix.mkdirTolerateExisting(ofsDir, DEFAULT_OFS_PERMISSION) != 0) {
            LOG.error("mkdirTolerateExisting failed on path f = {}, permission = {}", ofsDir,
                DEFAULT_OFS_PERMISSION);
            return false;
          }
        }
        return true;
      } catch (IOException e) {
        LOG.error("{} try to make directory for {} : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public InputStream open(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return new OrangeFileSystemInputStream(getOFSPath(path), mOfsBufferSize);
      } catch (IOException e) {
        LOG.error("{} try to open {} : {}", retryPolicy.getRetryCount(), path, e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    LOG.debug("Renaming from {} to {}", src, dst);
    if (!exists(src)) {
      LOG.error("File {} does not exist. Therefore rename to {} failed.", src, dst);
      return false;
    }

    if (exists(dst)) {
      LOG.error("File {} does exist. Therefore rename from {} failed.", dst, src);
      return false;
    }

    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        int ret = mOrange.posix.rename(getOFSPath(src), getOFSPath(dst));
        if (ret != 0) {
          LOG.debug("rename({}) returned null", mUri);
          throw new FileNotFoundException();
        }
        return ret == 0;
      } catch (IOException e) {
        LOG.error("{} try to rename {} to {} : {}", retryPolicy.getRetryCount(), src, dst,
            e.getMessage(), e);
      }
    }
    return false;
  }

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {
    if (!exists(path) || posixPerm == null) {
      return;
    }
    LOG.debug("permission (symbolic) = {}", posixPerm);
    String ofsPath = getOFSPath(path);
    int mode = Short.parseShort(posixPerm);

    if ((mode & 01000) == 01000) {
      LOG.warn("permission contains sticky bit, removing it...");
      mode = mode ^ 01000;
      LOG.warn("new mode = " + mode);
    }

    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        if (mOrange.posix.chmod(ofsPath, mode) < 0) {
          throw new IOException(
              "Failed to set permissions on path = " + ofsPath + ", mode = " + mode);
        }
      } catch (IOException e) {
        LOG.error("{} try to setPermission for {} with perm {}", path, posixPerm, e);
        te = e;
      }
    }
    throw te;
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param path The file name
   * @return File stats
   * @throws FileNotFoundException if an I/O error occurs
   * @throws IOException if an I/O error occurs
   */
  public Stat getFileStatus(String path) throws IOException {
    return getFileStatus(new AlluxioURI(path));
  }

  /**
   * Return a file status object that represents {@link AlluxioURI}.
   *
   * @param uri The file name with {@link AlluxioURI} form
   * @return File stats
   * @throws IOException if an I/O error occurs
   */
  public Stat getFileStatus(AlluxioURI uri) throws IOException {
    String ofsPath = getOFSPath(uri);
    LOG.debug("f = " + ofsPath);
    Stat stats = mOrange.posix.stat(ofsPath);

    // TODO(pfxuan): should find a good way to retry
    if (stats == null) {
      LOG.debug("stat({}) returned null", uri);
      return null;
    } else {
      return stats;
    }
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param path The file name
   * @return File stats
   * @throws IOException if an I/O error occurs
   */
  public Statfs getFileSystemStatus(String path) throws IOException {
    return getFileSystemStatus(new AlluxioURI(path));
  }

  /**
   * Return a file status object that represents {@link AlluxioURI}.
   *
   * @param uri The file name with {@link AlluxioURI} form
   * @return File stats
   * @throws IOException if an I/O error occurs
   */
  public Statfs getFileSystemStatus(AlluxioURI uri) throws IOException {
    Statfs statfs;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);

    while (retryPolicy.attemptRetry()) {
      try {
        String ofsPath = getOFSPath(uri);
        LOG.debug("f = {}", ofsPath);
        int fd = mOrange.posix.open(ofsPath, mOrange.posix.f.O_RDONLY, 0);
        statfs = mOrange.posix.fstatfs(fd);

        if (statfs == null) {
          LOG.debug("statfs({}) returned null", ofsPath);
          throw new FileNotFoundException();
        }

        return statfs;
      } catch (IOException e) {
        LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
      }
    }
    return null;
  }

  /**
   * Return a Path as a String that OrangeFS can use. ie. removes URI scheme and authority and
   * prepends ofsMount
   *
   * @param path The file name in Alluxio
   */
  private String getOFSPath(String path) {
    return getOFSPath(new AlluxioURI(path));
  }

  /**
   * Return a Path as a String that OrangeFS can use. ie. removes URI scheme and authority and
   * prepends ofsMount
   *
   * @param uri The file name with {@link AlluxioURI} form
   */
  private String getOFSPath(AlluxioURI uri) {
    String ret = mOfsMount + uri.getPath();
    LOG.debug("ret = {}", ret);
    return ret;
  }

  /**
   * Determines if the key represents a folder. If false is returned, it is not guaranteed that the
   * path exists.
   *
   * @param path the path to check
   * @return whether the given key identifies a folder
   * @throws IOException if an I/O error occurs
   */
  private boolean isFolder(String path) throws IOException {
    return isFolder(new AlluxioURI(path));
  }

  /**
   * Determines if the key represents a folder. If false is returned, it is not guaranteed that the
   * path exists.
   *
   * @param uri the {@link AlluxioURI} to check
   * @return whether the given Alluxio URI identifies a folder
   * @throws IOException if an I/O error occurs
   */
  private boolean isFolder(AlluxioURI uri) throws IOException {
    Stat stats = getFileStatus(uri);
    return (stats.st_mode & S_IFDIR) != 0;
  }
}
