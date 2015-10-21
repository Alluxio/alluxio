/*
 * Licensed to the University of California, Berkeley under one or more contributor license
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

package tachyon.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

/**
 * Base class for Apache Hadoop based Tachyon {@link FileSystem}. This class really just delegates
 * to {@link tachyon.client.TachyonFS} for most operations.
 *
 * All implementing classes must define {@link #isZookeeperMode()} which states if fault tolerant is
 * used and {@link #getScheme()} for Hadoop's {@link java.util.ServiceLoader} support.
 */
abstract class AbstractTFS extends FileSystem {
  public static final String FIRST_COM_PATH = "tachyon_dep/";
  public static final String RECOMPUTE_PATH = "tachyon_recompute/";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private String mUnderFSAddress;

  private URI mUri = null;
  private Path mWorkingDir = new Path(TachyonURI.SEPARATOR);
  private Statistics mStatistics = null;
  private TachyonFS mTFS = null;
  private String mTachyonHeader = null;
  private final TachyonConf mTachyonConf = ClientContext.getConf();

  @Override
  public FSDataOutputStream append(Path cPath, int bufferSize, Progressable progress)
      throws IOException {
    LOG.info("append(" + cPath + ", " + bufferSize + ", " + progress + ")");
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    long fileId = mTFS.getFileId(path);
    TachyonFile file = mTFS.getFile(fileId);

    if (file.length() > 0) {
      LOG.warn("This maybe an error.");
    }

    return new FSDataOutputStream(file.getOutStream(), mStatistics);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (mTFS != null) {
        mTFS.close();
      }
    }
  }

  /**
   * Attempts to create a file. Overwrite will not succeed if the path exists and is a folder.
   *
   * @param cPath path to create
   * @param permission permissions of the created file/folder
   * @param overwrite overwrite if file exists
   * @param bufferSize the size in bytes of the buffer to be used
   * @param replication under filesystem replication factor
   * @param blockSize block size in bytes
   * @param progress queryable progress
   * @return an FSDataOutputStream created at the indicated path of a file
   * @throws IOException if overwrite is not specified and the path already exists or if the path is
   *         a folder
   */
  @Override
  public FSDataOutputStream create(Path cPath, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    LOG.info("create(" + cPath + ", " + permission + ", " + overwrite + ", " + bufferSize + ", "
        + replication + ", " + blockSize + ", " + progress + ")");
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }

    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    if (mTFS.exist(path)) {
      if (overwrite && !mTFS.getFileStatus(-1, path).isFolder) {
        if (!mTFS.delete(path, false)) {
          throw new IOException("Failed to delete existing data " + cPath);
        }
      } else {
        throw new IOException(cPath.toString() + " already exists. Directories cannot be "
            + "overwritten with create.");
      }
    }
    long fileId = mTFS.createFile(path, blockSize);
    TachyonFile file = mTFS.getFile(fileId);
    file.setUFSConf(getConf());

    return new FSDataOutputStream(file.getOutStream(), mStatistics);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress reporting. Same as
   * create(), except fails if parent directory doesn't already exist.
   *
   * TODO(hy): We need to refactor this method after having a new internal API support (TACHYON-46).
   *
   * @param cPath the file name to open
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *        overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the size in bytes of the buffer to be used.
   * @param progress queryable progress
   * @throws IOException if 1) overwrite is not specified and the path already exists, 2) if the
   *         path is a folder, or 3) the parent directory does not exist
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path cPath, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath.getParent()));
    if (!mTFS.exist(path)) {
      throw new FileNotFoundException("Parent directory does not exist!");
    }
    return this.create(cPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  /**
   * Attempts to delete the file or directory with the specified path.
   *
   * @param cPath path to delete
   * @param recursive if true, will attempt to delete all children of the path
   * @return true if one or more files/directories were deleted; false otherwise
   * @throws IOException if the path failed to be deleted due to some constraint (ie. non empty
   *         directory with recursive flag disabled)
   */
  @Override
  public boolean delete(Path cPath, boolean recursive) throws IOException {
    LOG.info("delete(" + cPath + ", " + recursive + ")");
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    if (!mTFS.exist(path)) {
      return false;
    }
    boolean rtn = mTFS.delete(path, recursive);
    if (mTFS.exist(path)) {
      throw new IOException("Failed to delete path " + path.toString());
    }
    return rtn;
  }

  @Override
  public long getDefaultBlockSize() {
    return mTachyonConf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    if (file == null) {
      return null;
    }
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(file.getPath()));
    long fileId = mTFS.getFileId(path);
    if (fileId == -1) {
      throw new FileNotFoundException("File does not exist: " + file.getPath());
    }

    List<BlockLocation> blockLocations = new ArrayList<BlockLocation>();
    List<FileBlockInfo> blocks = mTFS.getFileBlocks(fileId);
    for (int k = 0; k < blocks.size(); k ++) {
      FileBlockInfo info = blocks.get(k);
      long offset = info.getOffset();
      long end = offset + info.blockInfo.getLength();
      // Check if there is any overlapping between [start, start+len] and [offset, end]
      if (end >= start && offset <= start + len) {
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<String> hosts = new ArrayList<String>();
        List<NetAddress> addrs = Lists.newArrayList();
        // add the existing in-memory block locations first
        for (tachyon.thrift.BlockLocation location : info.getBlockInfo().getLocations()) {
          addrs.add(location.getWorkerAddress());
        }
        // then add under file system location
        addrs.addAll(info.getUfsLocations());
        for (NetAddress addr : addrs) {
          // Name format is "hostname:data transfer port"
          String name = addr.host + ":" + addr.dataPort;
          LOG.debug("getFileBlockLocations : adding name : '" + name + "");
          names.add(name);
          hosts.add(addr.host);
        }
        blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names),
            CommonUtils.toStringArray(hosts), offset, info.blockInfo.getLength()));
      }
    }

    BlockLocation[] ret = new BlockLocation[blockLocations.size()];
    blockLocations.toArray(ret);
    return ret;
  }

  /**
   * {@inheritDoc}
   *
   * If the file does not exist in Tachyon, query it from HDFS.
   */
  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    TachyonURI tPath = new TachyonURI(Utils.getPathWithoutScheme(path));
    Path hdfsPath = Utils.getHDFSPath(tPath, mUnderFSAddress);

    LOG.info("getFileStatus(" + path + "): HDFS Path: " + hdfsPath + " TPath: " + mTachyonHeader
        + tPath);
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }
    TachyonFile file;
    try {
      file = mTFS.getFile(tPath);
    } catch (IOException ioe) {
      LOG.info("File does not exist: " + path);
      throw new FileNotFoundException("File does not exist: " + path);
    }

    FileStatus ret =
        new FileStatus(file.length(), file.isDirectory(), file.getDiskReplication(),
            file.getBlockSizeByte(), file.getCreationTimeMs(), file.getCreationTimeMs(), null,
            null, null, new Path(mTachyonHeader + tPath));
    return ret;
  }

  /**
   * Gets the URI schema that maps to the FileSystem. This was introduced in Hadoop 2.x as a means
   * to make loading new FileSystems simpler. This doesn't exist in Hadoop 1.x, so cannot put
   * @Override.
   *
   * @return schema hadoop should map to
   *
   * @see org.apache.hadoop.fs.FileSystem#createFileSystem(java.net.URI,
   *      org.apache.hadoop.conf.Configuration)
   */
  @Override
  public abstract String getScheme();

  /**
   * Returns an object implementing the Tachyon-specific client API.
   *
   * @return null if initialize() hasn't been called
   */
  public TachyonFS getTachyonFS() {
    return mTFS;
  }

  @Override
  public URI getUri() {
    return mUri;
  }

  @Override
  public Path getWorkingDirectory() {
    LOG.info("getWorkingDirectory: " + mWorkingDir);
    return mWorkingDir;
  }

  /**
   * {@inheritDoc}
   *
   * Sets up a lazy connection to Tachyon through mTFS.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    LOG.info("initialize(" + uri + ", " + conf + "). Connecting to Tachyon: " + uri.toString());
    Utils.addS3Credentials(conf);
    setConf(conf);
    mTachyonHeader = getScheme() + "://" + uri.getHost() + ":" + uri.getPort();

    // Set the statistics member. Use mStatistics instead of the parent class's variable.
    mStatistics = statistics;

    // Load TachyonConf if any and merge to the one in TachyonFS
    TachyonConf siteConf = ConfUtils.loadFromHadoopConfiguration(conf);
    if (siteConf != null) {
      mTachyonConf.merge(siteConf);
    }
    mTachyonConf.set(Constants.MASTER_HOSTNAME, uri.getHost());
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(uri.getPort()));
    mTachyonConf.set(Constants.ZOOKEEPER_ENABLED, Boolean.toString(isZookeeperMode()));

    mTFS = TachyonFS.get(mTachyonConf);

    mUri = URI.create(mTachyonHeader);
    mUnderFSAddress = mTFS.getUfsAddress();
    LOG.info(mTachyonHeader + " " + mUri + " " + mUnderFSAddress);
  }

  /**
   * Determines if zookeeper should be used for the FileSystem. This method should only be used for
   * {@link #initialize(java.net.URI, org.apache.hadoop.conf.Configuration)}.
   *
   * @return true if zookeeper should be used
   */
  protected abstract boolean isZookeeperMode();

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    TachyonURI tPath = new TachyonURI(Utils.getPathWithoutScheme(path));
    Path hdfsPath = Utils.getHDFSPath(tPath, mUnderFSAddress);
    LOG.info("listStatus(" + path + "): HDFS Path: " + hdfsPath);

    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    if (!mTFS.exist(tPath)) {
      throw new FileNotFoundException("File does not exist: " + path);
    }

    List<FileInfo> files = mTFS.listStatus(tPath);
    FileStatus[] ret = new FileStatus[files.size()];
    for (int k = 0; k < files.size(); k ++) {
      FileInfo info = files.get(k);
      // TODO(hy): Replicate 3 with the number of disk replications.
      ret[k] =
          new FileStatus(info.getLength(), info.isFolder, 3, info.getBlockSizeBytes(),
              info.getCreationTimeMs(), info.getCreationTimeMs(), null, null, null, new Path(
                  mTachyonHeader + info.getPath()));
    }
    return ret;
  }

  /**
   * Attempts to create a folder with the specified path. Parent directories will be created. Mkdirs
   * will fail if the path already exists or a parent is a file.
   *
   * @param cPath path to create
   * @param permission permissions to grant the created folder
   * @return true if the indicated folder is created successfully, false otherwise
   * @throws IOException if the folder cannot be created (e.g., it already exists)
   */
  @Override
  public boolean mkdirs(Path cPath, FsPermission permission) throws IOException {
    LOG.info("mkdirs(" + cPath + ", " + permission + ")");
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    return mTFS.mkdir(path);
  }

  /**
   * Attempts to open the specified file for reading.
   *
   * @param cPath the file name to open
   * @param bufferSize the size in bytes of the buffer to be used
   * @return an FSDataInputStream at the indicated path of a file
   * @throws IOException if the file cannot be opened (e.g., the path is a folder)
   */
  @Override
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.info("open(" + cPath + ", " + bufferSize + ")");
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    long fileId = mTFS.getFileId(path);

    return new FSDataInputStream(new HdfsFileInputStream(mTFS, fileId, Utils.getHDFSPath(path,
        mUnderFSAddress), getConf(), bufferSize, mStatistics, mTachyonConf));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename(" + src + ", " + dst + ")");
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }

    TachyonURI srcPath = new TachyonURI(Utils.getPathWithoutScheme(src));
    TachyonURI dstPath = new TachyonURI(Utils.getPathWithoutScheme(dst));
    FileInfo info;
    try {
      info = mTFS.getFileStatus(-1, dstPath);
    } catch (IOException ioe) {
      info = null;
    }
    // If the destination is an existing folder, try to move the src into the folder
    if (info != null && info.isFolder) {
      dstPath = dstPath.join(srcPath.getName());
    }
    try {
      return mTFS.rename(srcPath, dstPath);
    } catch (IOException ioe) {
      LOG.error("Failed to rename {} to {}", src, dst, ioe);
      return false;
    }
  }

  @Override
  public void setWorkingDirectory(Path path) {
    LOG.info("setWorkingDirectory(" + path + ")");
    if (path.isAbsolute()) {
      mWorkingDir = path;
    } else {
      mWorkingDir = new Path(mWorkingDir, path);
    }
  }
}
