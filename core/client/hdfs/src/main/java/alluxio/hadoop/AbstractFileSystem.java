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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.security.User;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.wire.FileBlockInfo;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;

/**
 * Base class for Apache Hadoop based Alluxio {@link org.apache.hadoop.fs.FileSystem}. This class
 * really just delegates to {@link alluxio.client.file.FileSystem} for most operations.
 *
 * All implementing classes must define {@link #isZookeeperMode()} which states if fault tolerant is
 * used and {@link #getScheme()} for Hadoop's {@link java.util.ServiceLoader} support.
 */
@NotThreadSafe
abstract class AbstractFileSystem extends org.apache.hadoop.fs.FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileSystem.class);

  public static final String FIRST_COM_PATH = "alluxio_dep/";
  // Always tell Hadoop that we have 3x replication.
  private static final int BLOCK_REPLICATION_CONSTANT = 3;
  /** Lock for initializing the contexts, currently only one set of contexts is supported. */
  private static final Object INIT_LOCK = new Object();

  /** Flag for if the contexts have been initialized. */
  @GuardedBy("INIT_LOCK")
  private static volatile boolean sInitialized = false;

  private FileSystemContext mContext = null;
  private FileSystem mFileSystem = null;

  private URI mUri = null;
  private Path mWorkingDir = new Path(AlluxioURI.SEPARATOR);
  private Statistics mStatistics = null;
  private String mAlluxioHeader = null;

  /**
   * Constructs a new {@link AbstractFileSystem} instance with specified a {@link FileSystem}
   * handler for tests.
   *
   * @param fileSystem handler to file system
   */
  @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  AbstractFileSystem(FileSystem fileSystem) {
    mFileSystem = fileSystem;
    sInitialized = true;
  }

  /**
   * Constructs a new {@link AbstractFileSystem} instance.
   */
  AbstractFileSystem() {}

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    LOG.debug("append({}, {}, {})", path, bufferSize, progress);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    try {
      if (mFileSystem.exists(uri)) {
        throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(uri));
      }
      return new FSDataOutputStream(mFileSystem.createFile(uri), mStatistics);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (mContext != null && mContext != FileSystemContext.INSTANCE) {
      mContext.close();
    }
    super.close();
  }

  /**
   * Attempts to create a file. Overwrite will not succeed if the path exists and is a folder.
   *
   * @param path path to create
   * @param permission permissions of the created file/folder
   * @param overwrite overwrite if file exists
   * @param bufferSize the size in bytes of the buffer to be used
   * @param replication under filesystem replication factor
   * @param blockSize block size in bytes
   * @param progress queryable progress
   * @return an {@link FSDataOutputStream} created at the indicated path of a file
   */
  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    LOG.debug("create({}, {}, {}, {}, {}, {}, {})", path, permission, overwrite, bufferSize,
        replication, blockSize, progress);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }

    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(blockSize)
        .setMode(new Mode(permission.toShort()));

    FileOutStream outStream;
    try {
      outStream = mFileSystem.createFile(uri, options);
    } catch (AlluxioException e) {
      //now we should consider the override parameter
      try {
        if (mFileSystem.exists(uri)) {
          if (!overwrite) {
            throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(uri));
          }
          if (mFileSystem.getStatus(uri).isFolder()) {
            throw new IOException(
                ExceptionMessage.FILE_CREATE_IS_DIRECTORY.getMessage(uri));
          }
          mFileSystem.delete(uri);
        }
        outStream = mFileSystem.createFile(uri, options);
      } catch (AlluxioException e2) {
        throw new IOException(e2);
      }
    }
    return new FSDataOutputStream(outStream, mStatistics);
  }

  /**
   * Opens an {@link FSDataOutputStream} at the indicated Path with write-progress reporting.
   * Same as {@link #create(Path, boolean, int, short, long, Progressable)}, except fails if parent
   * directory doesn't already exist.
   *
   * TODO(hy): We need to refactor this method after having a new internal API support (ALLUXIO-46).
   *
   * @param path the file name to open
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *        overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used
   * @param replication required block replication for the file
   * @param blockSize the size in bytes of the buffer to be used
   * @param progress queryable progress
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
          throws IOException {
    AlluxioURI parentUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path.getParent()));
    ensureExists(parentUri);
    return create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  /**
   * Attempts to delete the file or directory with the specified path.
   *
   * @param path path to delete
   * @return true if one or more files/directories were deleted; false otherwise
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
   * @param path path to delete
   * @param recursive if true, will attempt to delete all children of the path
   * @return true if one or more files/directories were deleted; false otherwise
   */
  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    LOG.debug("delete({}, {})", path, recursive);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
    try {
      mFileSystem.delete(uri, options);
      return true;
    } catch (InvalidPathException | FileDoesNotExistException e) {
      LOG.warn("delete failed: {}", e.getMessage());
      return false;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getDefaultBlockSize() {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Nullable
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
    List<FileBlockInfo> blocks = getFileBlocks(path);
    List<BlockLocation> blockLocations = new ArrayList<>();
    for (FileBlockInfo fileBlockInfo : blocks) {
      long offset = fileBlockInfo.getOffset();
      long end = offset + fileBlockInfo.getBlockInfo().getLength();
      // Check if there is any overlapping between [start, start+len] and [offset, end]
      if (end >= start && offset <= start + len) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<String> hosts = new ArrayList<>();
        // add the existing in-memory block locations
        for (alluxio.wire.BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
          HostAndPort address = HostAndPort.fromParts(location.getWorkerAddress().getHost(),
              location.getWorkerAddress().getDataPort());
          names.add(address.toString());
          hosts.add(address.getHostText());
        }
        // add under file system locations
        for (String location : fileBlockInfo.getUfsLocations()) {
          names.add(location);
          hosts.add(HostAndPort.fromString(location).getHostText());
        }
        blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names),
            CommonUtils.toStringArray(hosts), offset, fileBlockInfo.getBlockInfo().getLength()));
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
    LOG.debug("getFileStatus({})", path);

    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    URIStatus fileStatus;
    try {
      fileStatus = mFileSystem.getStatus(uri);
    } catch (FileDoesNotExistException e) {
      throw new FileNotFoundException(e.getMessage());
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    return new FileStatus(fileStatus.getLength(), fileStatus.isFolder(),
        BLOCK_REPLICATION_CONSTANT, fileStatus.getBlockSizeBytes(),
        fileStatus.getLastModificationTimeMs(),
        fileStatus.getCreationTimeMs(), new FsPermission((short) fileStatus.getMode()),
        fileStatus.getOwner(), fileStatus.getGroup(), new Path(mAlluxioHeader + uri));
  }

  /**
   * Changes owner or group of a path (i.e. a file or a directory). If username is null, the
   * original username remains unchanged. Same as groupname. If username and groupname are non-null,
   * both of them will be changed.
   *
   * @param path path to set owner or group
   * @param username username to be set
   * @param groupname groupname to be set
   */
  @Override
  public void setOwner(Path path, final String username, final String groupname)
      throws IOException {
    LOG.debug("setOwner({},{},{})", path, username, groupname);
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
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
      try {
        mFileSystem.setAttribute(uri, options);
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Changes permission of a path.
   *
   * @param path path to set permission
   * @param permission permission set to path
   */
  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    LOG.debug("setMode({},{})", path, permission.toString());
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setMode(new Mode(permission.toShort())).setRecursive(false);
    try {
      mFileSystem.setAttribute(uri, options);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets the URI scheme that maps to the {@link org.apache.hadoop.fs.FileSystem}. This was
   * introduced in Hadoop 2.x as a means to make loading new {@link org.apache.hadoop.fs.FileSystem}
   * s simpler. This doesn't exist in Hadoop 1.x, so cannot put {@literal @Override}.
   *
   * @return scheme hadoop should map to
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
    LOG.debug("getWorkingDirectory: {}", mWorkingDir);
    return mWorkingDir;
  }

  /**
   * {@inheritDoc}
   *
   * Sets up a lazy connection to Alluxio through mFileSystem. This method will override and
   * invalidate the current contexts. This must be called before client operations in order to
   * guarantee the integrity of the contexts, meaning users should not alternate between using the
   * Hadoop compatible API and native Alluxio API in the same process.
   *
   * If hadoop file system cache is enabled, this method should only be called when switching user.
   */
  @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  @Override
  public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
    super.initialize(uri, conf);
    LOG.debug("initialize({}, {}). Connecting to Alluxio", uri, conf);
    HadoopUtils.addS3Credentials(conf);
    HadoopUtils.addSwiftCredentials(conf);
    setConf(conf);

    // HDFS doesn't allow the authority to be empty; it must be "/" instead.
    String authority = uri.getAuthority() == null ? "/" : uri.getAuthority();
    mAlluxioHeader = getScheme() + "://" + authority;
    // Set the statistics member. Use mStatistics instead of the parent class's variable.
    mStatistics = statistics;
    mUri = URI.create(mAlluxioHeader);

    boolean masterAddIsSameAsDefault = checkMasterAddress();

    if (sInitialized && masterAddIsSameAsDefault) {
      updateFileSystemAndContext();
      return;
    }
    synchronized (INIT_LOCK) {
      // If someone has initialized the object since the last check, return
      if (sInitialized) {
        if (masterAddIsSameAsDefault) {
          updateFileSystemAndContext();
          return;
        } else {
          LOG.warn(ExceptionMessage.DIFFERENT_MASTER_ADDRESS
              .getMessage(mUri.getHost() + ":" + mUri.getPort(),
                  FileSystemContext.INSTANCE.getMasterAddress()));
          sInitialized = false;
        }
      }

      initializeInternal(uri, conf);
      sInitialized = true;
    }

    updateFileSystemAndContext();
  }

  /**
   * Initializes the default contexts if the master address specified in the URI is different
   * from the default one.
   *
   * @param uri the uri
   * @param conf the hadoop conf
   */
  void initializeInternal(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
    // Load Alluxio configuration if any and merge to the one in Alluxio file system. These
    // modifications to ClientContext are global, affecting all Alluxio clients in this JVM.
    // We assume here that all clients use the same configuration.
    HadoopConfigurationUtils.mergeHadoopConfiguration(conf);
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, isZookeeperMode());
    // When using zookeeper we get the leader master address from the alluxio.zookeeper.address
    // configuration property, so the user doesn't need to specify the authority.
    if (!Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      Preconditions.checkNotNull(uri.getHost(), PreconditionMessage.URI_HOST_NULL);
      Preconditions.checkNotNull(uri.getPort(), PreconditionMessage.URI_PORT_NULL);
      Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
      Configuration.set(PropertyKey.MASTER_RPC_PORT, uri.getPort());
    }

    // These must be reset to pick up the change to the master address.
    // TODO(andrew): We should reset key value system in this situation - see ALLUXIO-1706.
    LineageContext.INSTANCE.reset();
    FileSystemContext.INSTANCE.reset();

    // Try to connect to master, if it fails, the provided uri is invalid.
    FileSystemMasterClient client = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      client.connect();
      // Connected, initialize.
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(client);
    }
  }

  /**
   * Sets the file system and context.
   */
  private void updateFileSystemAndContext() {
    Subject subject = getHadoopSubject();
    if (subject != null) {
      mContext = FileSystemContext.create(subject);
      mFileSystem = FileSystem.Factory.get(mContext);
    } else {
      mContext = FileSystemContext.INSTANCE;
      mFileSystem = FileSystem.Factory.get();
    }
  }

  /**
   * @return true if the master address in mUri is the same as the one in the default file
   *         system context.
   */
  private boolean checkMasterAddress() {
    InetSocketAddress masterAddress = FileSystemContext.INSTANCE.getMasterAddress();
    boolean sameHost = masterAddress.getHostString().equals(mUri.getHost());
    boolean samePort = masterAddress.getPort() == mUri.getPort();
    if (sameHost && samePort) {
      return true;
    }
    return false;
  }

  /**
   * @return the hadoop subject if exists, null if not exist
   */
  @Nullable
  private Subject getHadoopSubject() {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getShortUserName();
      if (username != null && !username.isEmpty()) {
        User user = new User(ugi.getShortUserName());
        HashSet<Principal> principals = new HashSet<>();
        principals.add(user);
        return new Subject(false, principals, new HashSet<>(), new HashSet<>());
      }
      return null;
    } catch (IOException e) {
      return null;
    }
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
    LOG.debug("listStatus({})", path);

    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    List<URIStatus> statuses;
    try {
      statuses = mFileSystem.listStatus(uri);
    } catch (FileDoesNotExistException e) {
      throw new FileNotFoundException(HadoopUtils.getPathWithoutScheme(path));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }

    FileStatus[] ret = new FileStatus[statuses.size()];
    for (int k = 0; k < statuses.size(); k++) {
      URIStatus status = statuses.get(k);

      ret[k] = new FileStatus(status.getLength(), status.isFolder(), BLOCK_REPLICATION_CONSTANT,
          status.getBlockSizeBytes(), status.getLastModificationTimeMs(),
          status.getCreationTimeMs(), new FsPermission((short) status.getMode()), status.getOwner(),
          status.getGroup(), new Path(mAlluxioHeader + status.getPath()));
    }
    return ret;
  }

  /**
   * Attempts to create a folder with the specified path. Parent directories will be created.
   *
   * @param path path to create
   * @param permission permissions to grant the created folder
   * @return true if the indicated folder is created successfully or already exists
   */
  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    LOG.debug("mkdirs({}, {})", path, permission);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    CreateDirectoryOptions options =
        CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true)
            .setMode(new Mode(permission.toShort()));
    try {
      mFileSystem.createDirectory(uri, options);
      return true;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Attempts to open the specified file for reading.
   *
   * @param path the file name to open
   * @param bufferSize stream buffer size in bytes, currently unused
   * @return an {@link FSDataInputStream} at the indicated path of a file
   */
  // TODO(calvin): Consider respecting the buffer size option
  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    LOG.debug("open({}, {})", path, bufferSize);
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
    return new FSDataInputStream(new HdfsFileInputStream(mContext, uri, mStatistics));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("rename({}, {})", src, dst);
    if (mStatistics != null) {
      mStatistics.incrementWriteOps(1);
    }

    AlluxioURI srcPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(src));
    AlluxioURI dstPath = new AlluxioURI(HadoopUtils.getPathWithoutScheme(dst));
    try {
      mFileSystem.rename(srcPath, dstPath);
    } catch (FileDoesNotExistException e) {
      LOG.warn("rename failed: {}", e.getMessage());
      return false;
    } catch (AlluxioException e) {
      ensureExists(srcPath);
      URIStatus dstStatus;
      try {
        dstStatus = mFileSystem.getStatus(dstPath);
      } catch (IOException | AlluxioException e2) {
        LOG.warn("rename failed: {}", e.getMessage());
        return false;
      }
      // If the destination is an existing folder, try to move the src into the folder
      if (dstStatus != null && dstStatus.isFolder()) {
        dstPath = dstPath.join(srcPath.getName());
      } else {
        LOG.warn("rename failed: {}", e.getMessage());
        return false;
      }
      try {
        mFileSystem.rename(srcPath, dstPath);
      } catch (IOException | AlluxioException e2) {
        LOG.error("Failed to rename {} to {}", src, dst, e2);
        return false;
      }
    } catch (IOException e) {
      LOG.error("Failed to rename {} to {}", src, dst, e);
      return false;
    }
    return true;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    LOG.debug("setWorkingDirectory({})", path);
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
   */
  private void ensureExists(AlluxioURI path) throws IOException {
    try {
      mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  private List<FileBlockInfo> getFileBlocks(AlluxioURI path) throws IOException {
    try {
      return mFileSystem.getStatus(path).getFileBlockInfos();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }
}
