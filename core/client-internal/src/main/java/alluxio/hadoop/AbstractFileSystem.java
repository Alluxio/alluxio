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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.lineage.LineageContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.util.CommonUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Base class for Apache Hadoop based Alluxio {@link org.apache.hadoop.fs.FileSystem}. This class
 * really just delegates to {@link alluxio.client.file.FileSystem} for most operations.
 *
 * All implementing classes must define {@link #isZookeeperMode()} which states if fault tolerant is
 * used and {@link #getScheme()} for Hadoop's {@link java.util.ServiceLoader} support.
 */
@NotThreadSafe
abstract class AbstractFileSystem extends org.apache.hadoop.fs.FileSystem {
  public static final String FIRST_COM_PATH = "alluxio_dep/";
  public static final String RECOMPUTE_PATH = "alluxio_recompute/";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // Always tell Hadoop that we have 3x replication.
  private static final int BLOCK_REPLICATION_CONSTANT = 3;

  private String mUnderFSAddress;

  private URI mUri = null;
  private Path mWorkingDir = new Path(AlluxioURI.SEPARATOR);
  private Statistics mStatistics = null;
  private FileSystem mFileSystem = null;
  private String mAlluxioHeader = null;

  @Override
  public FSDataOutputStream append(Path cPath, int bufferSize, Progressable progress)
      throws IOException {
    LOG.info("append({}, {}, {})", cPath, bufferSize, progress);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(cPath));
    try {
      if (!mFileSystem.exists(path)) {
        return new FSDataOutputStream(mFileSystem.createFile(path), mStatistics);
      } else {
        throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
      }
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
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
   * @return an {@link FSDataOutputStream} created at the indicated path of a file
   * @throws IOException if overwrite is not specified and the path already exists or if the path is
   *         a folder
   */
  @Override
  public FSDataOutputStream create(Path cPath, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    LOG.info("create({}, {}, {}, {}, {}, {}, {})", cPath, permission, overwrite, bufferSize,
        replication, blockSize, progress);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }

    // Check whether the file already exists, and delete it if overwrite is true
    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(cPath));
    try {
      if (mFileSystem.exists(path)) {
        if (!overwrite) {
          throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(cPath.toString()));
        }
        if (mFileSystem.getStatus(path).isFolder()) {
          throw new IOException(
              ExceptionMessage.FILE_CREATE_IS_DIRECTORY.getMessage(cPath.toString()));
        }
        mFileSystem.delete(path);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    // The file no longer exists at this point, so we can create it
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(blockSize);
    try {
      FileOutStream outStream = mFileSystem.createFile(path, options);
      return new FSDataOutputStream(outStream, mStatistics);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Opens an {@link FSDataOutputStream} at the indicated Path with
   * write-progress reporting. Same as
   * {@link #create(Path, boolean, int, short, long, Progressable)}, except fails if parent
   * directory doesn't already exist.
   *
   * TODO(hy): We need to refactor this method after having a new internal API support (ALLUXIO-46).
   *
   * @param cPath the file name to open
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *        overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used
   * @param replication required block replication for the file
   * @param blockSize the size in bytes of the buffer to be used
   * @param progress queryable progress
   * @throws IOException if 1) overwrite is not specified and the path already exists, 2) if the
   *         path is a folder, or 3) the parent directory does not exist
   * @see {@link #setPermission(Path, FsPermission)}
   * @deprecated API only for 0.20-append
   */
  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path cPath, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
          throws IOException {
    AlluxioURI parentPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(cPath.getParent()));
    ensureExists(parentPath);
    return this.create(cPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  /**
   * Attempts to delete the file or directory with the specified path.
   *
   * @param path path to delete
   * @return true if one or more files/directories were deleted; false otherwise
   * @throws IOException if the path failed to be deleted due to some constraint
   * @deprecated Use {@link #delete(Path, boolean)} instead.
   */
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
    LOG.info("delete({}, {})", cPath, recursive);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(cPath));
    DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
    try {
      mFileSystem.delete(path, options);
      return true;
    } catch (InvalidPathException e) {
      LOG.info("delete failed: {}", e.getMessage());
      return false;
    } catch (FileDoesNotExistException e) {
      LOG.info("delete failed: {}", e.getMessage());
      return false;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getDefaultBlockSize() {
    return ClientContext.getConf().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
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

    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(file.getPath()));
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    List<FileBlockInfo> blocks = getFileBlocks(path);

    List<BlockLocation> blockLocations = new ArrayList<BlockLocation>();
    for (int k = 0; k < blocks.size(); k++) {
      FileBlockInfo info = blocks.get(k);
      long offset = info.getOffset();
      long end = offset + info.getBlockInfo().getLength();
      // Check if there is any overlapping between [start, start+len] and [offset, end]
      if (end >= start && offset <= start + len) {
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<String> hosts = new ArrayList<String>();
        List<WorkerNetAddress> addrs = Lists.newArrayList();
        // add the existing in-memory block locations first
        for (alluxio.wire.BlockLocation location : info.getBlockInfo().getLocations()) {
          addrs.add(location.getWorkerAddress());
        }
        // then add under file system location
        addrs.addAll(info.getUfsLocations());
        for (WorkerNetAddress addr : addrs) {
          // Name format is "hostname:data transfer port"
          String name = addr.getHost() + ":" + addr.getDataPort();
          LOG.debug("getFileBlockLocations : adding name : {}", name);
          names.add(name);
          hosts.add(addr.getHost());
        }
        blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names),
            CommonUtils.toStringArray(hosts), offset, info.getBlockInfo().getLength()));
      }
    }

    BlockLocation[] ret = new BlockLocation[blockLocations.size()];
    blockLocations.toArray(ret);
    return ret;
  }

  /**
   * {@inheritDoc}
   *
   * If the file does not exist in Alluxio, query it from HDFS.
   */
  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    AlluxioURI tPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    Path hdfsPath = HadoopUtils.getHDFSPath(tPath, mUnderFSAddress);

    LOG.info("getFileStatus({}): HDFS Path: {} Alluxio Path: {}{}", path, hdfsPath, mAlluxioHeader,
        tPath);
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }
    URIStatus fileStatus;
    try {
      fileStatus = mFileSystem.getStatus(tPath);
    } catch (FileDoesNotExistException e) {
      throw new FileNotFoundException(e.getMessage());
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    FileStatus ret = new FileStatus(fileStatus.getLength(), fileStatus.isFolder(),
        BLOCK_REPLICATION_CONSTANT, fileStatus.getBlockSizeBytes(), fileStatus.getCreationTimeMs(),
            fileStatus.getCreationTimeMs(), new FsPermission((short) fileStatus.getPermission()),
            fileStatus.getUserName(), fileStatus.getGroupName(), new Path(mAlluxioHeader + tPath));
    return ret;
  }

  /**
   * Changes owner or group of a path (i.e. a file or a directory). If username is null, the
   * original username remains unchanged. Same as groupname. If username and groupname are non-null,
   * both of them will be changed.
   *
   * @param path path to set owner or group
   * @param username username to be set
   * @param groupname groupname to be set
   * @throws IOException if changing owner or group of the path failed
   */
  @Override
  public void setOwner(Path path, final String username, final String groupname)
      throws IOException {
    AlluxioURI tPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    Path hdfsPath = HadoopUtils.getHDFSPath(tPath, mUnderFSAddress);
    LOG.info("setOwner({},{},{}) HDFS Path: {} Alluxio Path: {}{}", path, username, groupname,
        hdfsPath, mAlluxioHeader, tPath);
    try {
      SetAttributeOptions options = SetAttributeOptions.defaults();
      boolean ownerOrGroupChanged = false;
      if (username != null && !username.isEmpty()) {
        options.setOwner(username).setRecursive(false);
        ownerOrGroupChanged = true;
      }
      if (groupname != null && !groupname.isEmpty()) {
        options.setGroup(groupname).setRecursive(false);
        ownerOrGroupChanged = true;
      }
      if (ownerOrGroupChanged) {
        mFileSystem.setAttribute(tPath, options);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Changes permission of a path.
   *
   * @param path path to set permission
   * @param permission permission set to path
   * @throws IOException if the path failed to be changed permission
   */
  public void setPermission(Path path, FsPermission permission) throws IOException {
    AlluxioURI tPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    Path hdfsPath = HadoopUtils.getHDFSPath(tPath, mUnderFSAddress);
    LOG.info("setPermission({},{}) HDFS Path: {} Alluxio Path: {}{}", path, permission.toString(),
        hdfsPath, mAlluxioHeader, tPath);
    try {
      SetAttributeOptions options =
          SetAttributeOptions.defaults().setPermission(permission.toShort()).setRecursive(false);
      mFileSystem.setAttribute(tPath, options);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets the URI schema that maps to the {@link org.apache.hadoop.fs.FileSystem}. This was
   * introduced in Hadoop 2.x as a means to make loading new {@link org.apache.hadoop.fs.FileSystem}
   * s simpler. This doesn't exist in Hadoop 1.x, so cannot put {@literal @Override}.
   *
   * @return schema hadoop should map to
   *
   * @see org.apache.hadoop.fs.FileSystem#createFileSystem(java.net.URI,
   *      org.apache.hadoop.conf.Configuration)
   */
  public abstract String getScheme();

  @Override
  public URI getUri() {
    return mUri;
  }

  @Override
  public Path getWorkingDirectory() {
    LOG.info("getWorkingDirectory: {}", mWorkingDir);
    return mWorkingDir;
  }

  /**
   * {@inheritDoc}
   *
   * Sets up a lazy connection to Alluxio through mFileSystem.
   */
  @Override
  public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
    Preconditions.checkNotNull(uri.getHost(), PreconditionMessage.URI_HOST_NULL);
    Preconditions.checkNotNull(uri.getPort(), PreconditionMessage.URI_PORT_NULL);
    super.initialize(uri, conf);
    LOG.info("initialize({}, {}). Connecting to Alluxio: {}", uri, conf, uri.toString());
    HadoopUtils.addS3Credentials(conf);
    setConf(conf);
    mAlluxioHeader = getScheme() + "://" + uri.getHost() + ":" + uri.getPort();

    // Set the statistics member. Use mStatistics instead of the parent class's variable.
    mStatistics = statistics;

    // Load Alluxio configuration if any and merge to the one in Alluxio file system
    Configuration siteConf = ConfUtils.loadFromHadoopConfiguration(conf);
    // These modifications to ClientContext are global, affecting every Alluxio client in this JVM.
    // We assume here that this client is the only client.
    if (siteConf != null) {
      ClientContext.getConf().merge(siteConf);
    }
    ClientContext.getConf().set(Constants.MASTER_HOSTNAME, uri.getHost());
    ClientContext.getConf().set(Constants.MASTER_RPC_PORT, Integer.toString(uri.getPort()));
    ClientContext.getConf().set(Constants.ZOOKEEPER_ENABLED, Boolean.toString(isZookeeperMode()));

    // These must be reset to pick up the change to the master address.
    // TODO(andrew): We should reset key value system in this situation - see ALLUXIO-1706.
    ClientContext.init();
    FileSystemContext.INSTANCE.reset();
    BlockStoreContext.INSTANCE.reset();
    LineageContext.INSTANCE.reset();

    mFileSystem = FileSystem.Factory.get();
    mUri = URI.create(mAlluxioHeader);
    mUnderFSAddress = getUfsAddress();
    LOG.info("{} {} {}", mAlluxioHeader, mUri, mUnderFSAddress);
  }

  /**
   * Determines if zookeeper should be used for the {@link org.apache.hadoop.fs.FileSystem}. This
   * method should only be used for
   * {@link #initialize(java.net.URI, org.apache.hadoop.conf.Configuration)}.
   *
   * @return true if zookeeper should be used
   */
  protected abstract boolean isZookeeperMode();

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    AlluxioURI tPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    Path hdfsPath = HadoopUtils.getHDFSPath(tPath, mUnderFSAddress);
    LOG.info("listStatus({}): HDFS Path: {}", path, hdfsPath);

    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    List<URIStatus> statuses;
    try {
      statuses = mFileSystem.listStatus(tPath);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    FileStatus[] ret = new FileStatus[statuses.size()];
    for (int k = 0; k < statuses.size(); k++) {
      URIStatus status = statuses.get(k);
      // TODO(hy): Replicate 3 with the number of disk replications.
      ret[k] = new FileStatus(status.getLength(), status.isFolder(), 3, status.getBlockSizeBytes(),
          status.getCreationTimeMs(), status.getCreationTimeMs(), null, null, null,
          new Path(mAlluxioHeader + status.getPath()));
    }
    return ret;
  }

  /**
   * Attempts to create a folder with the specified path. Parent directories will be created.
   *
   * @param cPath path to create
   * @param permission permissions to grant the created folder
   * @return true if the indicated folder is created successfully or already exists
   * @throws IOException if the folder cannot be created
   */
  @Override
  public boolean mkdirs(Path cPath, FsPermission permission) throws IOException {
    LOG.info("mkdirs({}, {})", cPath, permission);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(cPath));
    CreateDirectoryOptions options =
        CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true);
    try {
      mFileSystem.createDirectory(path, options);
      return true;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Attempts to open the specified file for reading.
   *
   * @param cPath the file name to open
   * @param bufferSize the size in bytes of the buffer to be used
   * @return an {@link FSDataInputStream} at the indicated path of a file
   * @throws IOException if the file cannot be opened (e.g., the path is a folder)
   */
  @Override
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.info("open({}, {})", cPath, bufferSize);
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(cPath));
    return new FSDataInputStream(new HdfsFileInputStream(path, HadoopUtils.getHDFSPath(path,
            mUnderFSAddress), getConf(), bufferSize, mStatistics));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename({}, {})", src, dst);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }

    AlluxioURI srcPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(src));
    AlluxioURI dstPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(dst));
    ensureExists(srcPath);
    URIStatus dstStatus;
    try {
      dstStatus = mFileSystem.getStatus(dstPath);
    } catch (IOException e) {
      dstStatus = null;
    } catch (AlluxioException e) {
      dstStatus = null;
    }
    // If the destination is an existing folder, try to move the src into the folder
    if (dstStatus != null && dstStatus.isFolder()) {
      dstPath = dstPath.join(srcPath.getName());
    }
    try {
      mFileSystem.rename(srcPath, dstPath);
      return true;
    } catch (IOException e) {
      LOG.error("Failed to rename {} to {}", src, dst, e);
      return false;
    } catch (AlluxioException e) {
      LOG.error("Failed to rename {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  public void setWorkingDirectory(Path path) {
    LOG.info("setWorkingDirectory({})", path);
    if (path.isAbsolute()) {
      mWorkingDir = path;
    } else {
      mWorkingDir = new Path(mWorkingDir, path);
    }
  }

  /**
   * Convenience method which ensures the given path exists, wrapping any {@link AlluxioException}
   * in {@link IOException}.
   *
   * @param path the path to look up
   * @throws IOException if an Alluxio exception occurs
   */
  private void ensureExists(AlluxioURI path) throws IOException {
    try {
      mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  private List<FileBlockInfo> getFileBlocks(AlluxioURI path) throws IOException {
    FileSystemMasterClient master = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return master.getFileBlockInfoList(path);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(master);
    }
  }

  private String getUfsAddress() throws IOException {
    FileSystemMasterClient master = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      return master.getUfsAddress();
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(master);
    }
  }
}
