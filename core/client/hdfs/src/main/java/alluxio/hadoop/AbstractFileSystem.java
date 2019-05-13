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

import static java.util.stream.Collectors.toList;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterInquireClient.Factory;
import alluxio.security.CurrentUser;
import alluxio.security.authorization.Mode;
import alluxio.uri.Authority;
import alluxio.uri.MultiMasterAuthority;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.UnknownAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

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
import java.net.URI;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
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

  protected FileSystem mFileSystem = null;

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
      return new FSDataOutputStream(
          mFileSystem.createFile(uri, CreateFilePOptions.newBuilder().setRecursive(true).build()),
          mStatistics);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // super.close should be called first before releasing the resources in this instance, as the
    // super class may invoke other methods in this class. For example,
    // org.apache.hadoop.fs.FileSystem.close may check the existence of certain temp files before
    // closing
    super.close();
    mFileSystem.close();
  }

  /**
   * Attempts to create a file. Overwrite will not succeed if the path exists and is a folder.
   *
   * @param path path to create
   * @param permission permissions of the created file/folder
   * @param overwrite overwrite if file exists
   * @param bufferSize the size in bytes of the buffer to be used
   * @param replication under filesystem replication factor, this is ignored
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
    CreateFilePOptions options = CreateFilePOptions.newBuilder().setBlockSizeBytes(blockSize)
        .setMode(new Mode(permission.toShort()).toProto()).setRecursive(true).build();

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
    DeletePOptions options = DeletePOptions.newBuilder().setRecursive(recursive).build();
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
    return mFileSystem.getConf()
        .getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Nullable
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    LOG.debug("getFileBlockLocations({}, {}, {})", file.getPath().getName(), start, len);
    if (file == null) {
      LOG.debug("getFileBlockLocations({}, {}, {}) returned null",
          file.getPath().getName(), start, len);
      return null;
    }
    if (mStatistics != null) {
      mStatistics.incrementReadOps(1);
    }

    List<BlockLocation> blockLocations = new ArrayList<>();
    AlluxioURI path = new AlluxioURI(HadoopUtils.getPathWithoutScheme(file.getPath()));
    try {
      List<BlockLocationInfo> locations = mFileSystem.getBlockLocations(path);
      locations.forEach(location -> {
        FileBlockInfo info = location.getBlockInfo();
        List<WorkerNetAddress> workers = location.getLocations();
        long offset = location.getBlockInfo().getOffset();
        long end = offset + info.getBlockInfo().getLength();
        if (end >= start && offset <= start + len) {
          List<HostAndPort> addresses = workers.stream()
              .map(worker -> HostAndPort.fromParts(worker.getHost(), worker.getDataPort()))
              .collect(toList());
          String[] names = addresses.stream().map(HostAndPort::toString).toArray(String[]::new);
          String[] hosts = addresses.stream().map(HostAndPort::getHost).toArray(String[]::new);
          blockLocations.add(new BlockLocation(names, hosts, offset,
              info.getBlockInfo().getLength()));
        }
      });
      BlockLocation[] ret = blockLocations.toArray(new BlockLocation[blockLocations.size()]);
      if (LOG.isDebugEnabled()) {
        LOG.debug("getFileBlockLocations({}, {}, {}) returned {}",
            file.getPath().getName(), start, len, Arrays.toString(ret));
      }
      return ret;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public short getDefaultReplication() {
    return (short) Math.max(1, mFileSystem.getConf()
        .getInt(PropertyKey.USER_FILE_REPLICATION_MIN));
  }

  @Override
  public boolean setReplication(Path path, short replication) throws IOException {
    AlluxioURI uri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));

    try {
      if (!mFileSystem.exists(uri) || mFileSystem.getStatus(uri).isFolder()) {
        return false;
      }
      mFileSystem.setAttribute(uri, (SetAttributePOptions) SetAttributePOptions.newBuilder()
          .setReplicationMin(replication).build());
      return true;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
        getReplica(fileStatus), fileStatus.getBlockSizeBytes(),
        fileStatus.getLastModificationTimeMs(),
        fileStatus.getCreationTimeMs(), new FsPermission((short) fileStatus.getMode()),
        fileStatus.getOwner(), fileStatus.getGroup(), new Path(mAlluxioHeader + uri));
  }

  private int getReplica(URIStatus status) {
    return status.getReplicationMin();
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
    SetAttributePOptions.Builder optionsBuilder = SetAttributePOptions.newBuilder();
    boolean ownerOrGroupChanged = false;
    if (username != null && !username.isEmpty()) {
      optionsBuilder.setOwner(username).setRecursive(false);
      ownerOrGroupChanged = true;
    }
    if (groupname != null && !groupname.isEmpty()) {
      optionsBuilder.setGroup(groupname).setRecursive(false);
      ownerOrGroupChanged = true;
    }
    if (ownerOrGroupChanged) {
      try {
        mFileSystem.setAttribute(uri, optionsBuilder.build());
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
    SetAttributePOptions options = SetAttributePOptions.newBuilder()
        .setMode(new Mode(permission.toShort()).toProto()).setRecursive(false).build();
    try {
      mFileSystem.setAttribute(uri, options);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets the URI scheme that maps to the {@link org.apache.hadoop.fs.FileSystem}. This was
   * introduced in Hadoop 2.x as a means to make loading new {@link org.apache.hadoop.fs.FileSystem}
   * s simpler.
   *
   * @return scheme hadoop should map to
   *
   * @see org.apache.hadoop.fs.FileSystem#createFileSystem(java.net.URI,
   *      org.apache.hadoop.conf.Configuration)
   */
  //@Override This doesn't exist in Hadoop 1.x, so cannot put {@literal @Override}.
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
   * Sets up a lazy connection to Alluxio through mFileSystem. This must be called before client
   * operations.
   *
   * If it is called twice on the same object concurrently, one of the thread will do the
   * initialization work, the other will wait until initialization is done.
   * If it is called after initialized, then this is a noop.
   */
  @Override
  public synchronized void initialize(URI uri, org.apache.hadoop.conf.Configuration conf)
      throws IOException {
    initialize(uri, conf, null);
  }

  /**
   * Initialize the {@link alluxio.hadoop.FileSystem}.
   * @param uri file system Uri
   * @param conf hadoop configuration
   * @param alluxioConfiguration [optional] alluxio configuration
   * @throws IOException
   */
  public synchronized void initialize(URI uri, org.apache.hadoop.conf.Configuration conf,
      @Nullable AlluxioConfiguration alluxioConfiguration)
      throws IOException {
    Preconditions.checkArgument(uri.getScheme().equals(getScheme()),
        PreconditionMessage.URI_SCHEME_MISMATCH.toString(), uri.getScheme(), getScheme());

    super.initialize(uri, conf);
    LOG.debug("initialize({}, {}). Connecting to Alluxio", uri, conf);
    HadoopUtils.addSwiftCredentials(conf);
    setConf(conf);

    // HDFS doesn't allow the authority to be empty; it must be "/" instead.
    String authority = uri.getAuthority() == null ? "/" : uri.getAuthority();
    mAlluxioHeader = getScheme() + "://" + authority;
    // Set the statistics member. Use mStatistics instead of the parent class's variable.
    mStatistics = statistics;

    Authority auth = Authority.fromString(uri.getAuthority());
    if (auth instanceof UnknownAuthority) {
      throw new IOException(String.format("Authority \"%s\" is unknown. The client can not be "
              + "configured with the authority from %s", auth, uri));
    }

    mUri = URI.create(mAlluxioHeader);

    if (mFileSystem != null) {
      return;
    }

    Map<String, Object> uriConfProperties = getConfigurationFromUri(uri);

    AlluxioProperties alluxioProps =
        (alluxioConfiguration != null) ? alluxioConfiguration.copyProperties()
            : ConfigurationUtils.defaults();
    AlluxioConfiguration alluxioConf = mergeConfigurations(uriConfProperties, conf, alluxioProps);
    Subject subject = getHadoopSubject();
    LOG.debug("Using Hadoop subject: {}", subject);

    LOG.info("Initializing filesystem with connect details {}",
        Factory.getConnectDetails(alluxioConf));

    mFileSystem = FileSystem.Factory.create(ClientContext.create(subject, alluxioConf));
  }

  /**
   * Merges the URI configuration with the Hadoop and Alluxio configuration, returning an
   * {@link AlluxioConfiguration} with all properties merged into one object.
   *
   * @param uriConfProperties the configuration properties from the input uri
   * @param conf the hadoop conf
   * @param alluxioProps Alluxio configuration properties
   */
  private AlluxioConfiguration mergeConfigurations(Map<String, Object> uriConfProperties,
      org.apache.hadoop.conf.Configuration conf, AlluxioProperties alluxioProps)
      throws IOException {
    // take the URI properties, hadoop configuration, and given Alluxio configuration and merge
    // all three into a single object.
    InstancedConfiguration newConf = HadoopConfigurationUtils.mergeHadoopConfiguration(conf,
        alluxioProps);
    // Connection details in the URI has the highest priority
    newConf.merge(uriConfProperties, Source.RUNTIME);
    return newConf;
  }

  /**
   * Gets the connection configuration from the input uri.
   *
   * @param uri a Alluxio Uri that may contain connection configuration
   */
  private Map<String, Object> getConfigurationFromUri(URI uri) {
    AlluxioURI alluxioUri = new AlluxioURI(uri.toString());
    Map<String, Object> alluxioConfProperties = new HashMap<>();

    if (alluxioUri.getAuthority() instanceof ZookeeperAuthority) {
      ZookeeperAuthority authority = (ZookeeperAuthority) alluxioUri.getAuthority();
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), true);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(),
          authority.getZookeeperAddress());
    } else if (alluxioUri.getAuthority() instanceof SingleMasterAuthority) {
      SingleMasterAuthority authority = (SingleMasterAuthority) alluxioUri.getAuthority();
      alluxioConfProperties.put(PropertyKey.MASTER_HOSTNAME.getName(), authority.getHost());
      alluxioConfProperties.put(PropertyKey.MASTER_RPC_PORT.getName(), authority.getPort());
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
      // Unset the embedded journal related configuration
      // to support alluxio URI has the highest priority
      alluxioConfProperties.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES.getName(), null);
      alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(), null);
    } else if (alluxioUri.getAuthority() instanceof MultiMasterAuthority) {
      MultiMasterAuthority authority = (MultiMasterAuthority) alluxioUri.getAuthority();
      alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(),
          authority.getMasterAddresses());
      // Unset the zookeeper configuration to support alluxio URI has the highest priority
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
      alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
    }
    return alluxioConfProperties;
  }

  private Subject getSubjectFromUGI(UserGroupInformation ugi)
      throws IOException, InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<Subject>) () -> {
      AccessControlContext context = AccessController.getContext();
      return Subject.getSubject(context);
    });
  }

  /**
   * @return hadoop UGI's subject, or a fresh subject if the Hadoop UGI does not exist
   * @throws IOException if there is an exception when accessing the subject in Hadoop UGI
   */
  private Subject getHadoopSubject() throws IOException {
    Subject subject = null;
    UserGroupInformation ugi = null;
    try {
      ugi = UserGroupInformation.getCurrentUser();
      subject = getSubjectFromUGI(ugi);
    } catch (Exception e) {
      throw new IOException(
          String.format("Failed to get Hadoop subject for the Alluxio client. ugi: %s", ugi), e);
    }
    if (subject == null) {
      LOG.warn("Hadoop subject does not exist. Creating a fresh subject for Alluxio client");
      subject = new Subject(false, new HashSet<>(), new HashSet<>(), new HashSet<>());
    }
    if (subject.getPrincipals(CurrentUser.class).isEmpty() && ugi != null) {
      subject.getPrincipals().add(new CurrentUser(ugi.getShortUserName(), mUri.toString()));
    }
    return subject;
  }

  /**
   * @deprecated in 1.6.0, directly infer the value from {@link PropertyKey#ZOOKEEPER_ENABLED}
   * configuration value.
   *
   * Determines if zookeeper should be used for the {@link org.apache.hadoop.fs.FileSystem}. This
   * method should only be used for
   * {@link #initialize(java.net.URI, org.apache.hadoop.conf.Configuration)}.
   *
   * @return true if zookeeper should be used
   */
  @Deprecated
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

      ret[k] = new FileStatus(status.getLength(), status.isFolder(), getReplica(status),
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
    CreateDirectoryPOptions options = CreateDirectoryPOptions.newBuilder().setRecursive(true)
        .setAllowExists(true).setMode(new Mode(permission.toShort()).toProto()).build();
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
    return new FSDataInputStream(new HdfsFileInputStream(mFileSystem, uri, mStatistics));
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
        dstPath = dstPath.joinUnsafe(srcPath.getName());
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
}
