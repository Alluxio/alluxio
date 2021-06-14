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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.SyncInfo;
import alluxio.UfsConstants;
import alluxio.conf.PropertyKey;
import alluxio.collections.Pair;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.ConsistentUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * HDFS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class HdfsUnderFileSystem extends ConsistentUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUnderFileSystem.class);
  private static final int MAX_TRY = 5;
  private static final String HDFS_USER = "";
  /** Name of the class for the HDFS Acl provider. */
  private static final String HDFS_ACL_PROVIDER_CLASS =
      "alluxio.underfs.hdfs.acl.SupportedHdfsAclProvider";

  /** Name of the class for the Hdfs ActiveSync provider. */
  private static final String HDFS_ACTIVESYNC_PROVIDER_CLASS =
      "alluxio.underfs.hdfs.activesync.SupportedHdfsActiveSyncProvider";

  /** The minimum HDFS production version required for EC. **/
  private static final String HDFS_EC_MIN_VERSION = "3.0.0";

  /** Name of the class for the HDFS EC Codec Registry. **/
  private static final String HDFS_EC_CODEC_REGISTRY_CLASS =
      "org.apache.hadoop.io.erasurecode.CodecRegistry";

  private final LoadingCache<String, FileSystem> mUserFs;
  private final HdfsAclProvider mHdfsAclProvider;

  private HdfsActiveSyncProvider mHdfsActiveSyncer;

  /**
   * Factory method to constructs a new HDFS {@link UnderFileSystem} instance.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop
   * @return a new HDFS {@link UnderFileSystem} instance
   */
  public static HdfsUnderFileSystem createInstance(AlluxioURI ufsUri,
      UnderFileSystemConfiguration conf) {
    Configuration hdfsConf = createConfiguration(conf);
    return new HdfsUnderFileSystem(ufsUri, conf, hdfsConf);
  }

  /**
   * Constructs a new HDFS {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param hdfsConf the configuration for HDFS
   */
  public HdfsUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      Configuration hdfsConf) {
    super(ufsUri, conf);

    // Create the supported HdfsAclProvider if possible.
    HdfsAclProvider hdfsAclProvider = new NoopHdfsAclProvider();
    try {
      // The HDFS acl provider class may not be available, so the class must be created from a
      // string literal.
      Object o = Class.forName(HDFS_ACL_PROVIDER_CLASS).newInstance();
      if (o instanceof HdfsAclProvider) {
        hdfsAclProvider = (HdfsAclProvider) o;
      } else {
        LOG.warn(
            "SupportedHdfsAclProvider is not instance of HdfsAclProvider. HDFS ACLs will not be "
                + "supported.");
      }
    } catch (Exception e) {
      // ignore
      LOG.warn("Cannot create SupportedHdfsAclProvider. HDFS ACLs will not be supported.");
    }
    mHdfsAclProvider = hdfsAclProvider;

    Path path = new Path(ufsUri.toString());
    // UserGroupInformation.setConfiguration(hdfsConf) will trigger service loading.
    // Stash the classloader to prevent service loading throwing exception due to
    // classloader mismatch.
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(hdfsConf.getClassLoader());
      // Set Hadoop UGI configuration to ensure UGI can be initialized by the shaded classes for
      // group service.
      UserGroupInformation.setConfiguration(hdfsConf);
      // When HDFS version is 3.0.0 or later, initialize HDFS EC CodecRegistry here to ensure
      // RawErasureCoderFactory implementations are loaded by the same classloader of hdfsConf.
      if (UfsConstants.UFS_HADOOP_VERSION.compareTo(HDFS_EC_MIN_VERSION) >= 0) {
        try {
          Class.forName(HDFS_EC_CODEC_REGISTRY_CLASS);
        } catch (ClassNotFoundException e) {
          LOG.warn("Cannot initialize HDFS EC CodecRegistry. "
              + "HDFS EC will not be supported: {}", e.toString());
        }
      }
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }

    mUserFs = CacheBuilder.newBuilder().build(new CacheLoader<String, FileSystem>() {
      @Override
      public FileSystem load(String userKey) throws Exception {
        // When running {@link UnderFileSystemContractTest} with hdfs path,
        // the org.apache.hadoop.fs.FileSystem is loaded by {@link ExtensionClassLoader},
        // but the org.apache.hadoop.fs.LocalFileSystem is loaded by {@link AppClassLoader}.
        // When an interface and associated implementation are each loaded
        // by two separate class loaders, an instance of the class from one loader cannot
        // be recognized as implementing the interface from the other loader.
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
        try {
          // Set the class loader to ensure FileSystem implementations are
          // loaded by the same class loader to avoid ServerConfigurationError
          Thread.currentThread().setContextClassLoader(currentClassLoader);
          return path.getFileSystem(hdfsConf);
        } finally {
          Thread.currentThread().setContextClassLoader(previousClassLoader);
        }
      }
    });

    // Create the supported HdfsActiveSyncer if possible.
    HdfsActiveSyncProvider hdfsActiveSyncProvider = new NoopHdfsActiveSyncProvider();

    try {
      Constructor c = Class.forName(HDFS_ACTIVESYNC_PROVIDER_CLASS)
          .getConstructor(URI.class, Configuration.class, UnderFileSystemConfiguration.class);
      Object o = c.newInstance(URI.create(ufsUri.toString()), hdfsConf, mUfsConf);
      if (o instanceof HdfsActiveSyncProvider) {
        hdfsActiveSyncProvider = (HdfsActiveSyncProvider) o;
        LOG.info("Successfully instantiated SupportedHdfsActiveSyncProvider");
      } else {
        LOG.warn(
            "SupportedHdfsActiveSyncProvider is not instance of HdfsActiveSyncProvider. "
                + "HDFS ActiveSync will not be supported.");
      }
    } catch (Exception e) {
      // ignore
      LOG.warn("Cannot create SupportedHdfsActiveSyncProvider."
          + "HDFS ActiveSync will not be supported.");
    }

    mHdfsActiveSyncer = hdfsActiveSyncProvider;
  }

  @Override
  public String getUnderFSType() {
    return "hdfs";
  }

  /**
   * Prepares the Hadoop configuration necessary to successfully obtain a {@link FileSystem}
   * instance that can access the provided path.
   * <p>
   * Derived implementations that work with specialised Hadoop {@linkplain FileSystem} API
   * compatible implementations can override this method to add implementation specific
   * configuration necessary for obtaining a usable {@linkplain FileSystem} instance.
   * </p>
   *
   * @param conf the configuration for this UFS
   * @return the configuration for HDFS
   */
  public static Configuration createConfiguration(UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(conf, "conf");
    Configuration hdfsConf = new Configuration();

    // Load HDFS site properties from the given file and overwrite the default HDFS conf,
    // the path of this file can be passed through --option
    for (String path : conf.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION).split(":")) {
      if (!path.isEmpty()) {
        hdfsConf.addResource(new Path(path));
      }
    }

    // On Hadoop 2.x this is strictly unnecessary since it uses ServiceLoader to automatically
    // discover available file system implementations. However this configuration setting is
    // required for earlier Hadoop versions plus it is still honoured as an override even in 2.x so
    // if present propagate it to the Hadoop configuration
    String ufsHdfsImpl = conf.get(PropertyKey.UNDERFS_HDFS_IMPL);
    if (!StringUtils.isEmpty(ufsHdfsImpl)) {
      hdfsConf.set("fs.hdfs.impl", ufsHdfsImpl);
    }

    // Disable HDFS client caching so that input configuration is respected. Configurable from
    // system property
    hdfsConf.set("fs.hdfs.impl.disable.cache",
        System.getProperty("fs.hdfs.impl.disable.cache", "true"));

    // Set all parameters passed through --option
    for (Map.Entry<String, String> entry : conf.getMountSpecificConf().entrySet()) {
      hdfsConf.set(entry.getKey(), entry.getValue());
    }
    return hdfsConf;
  }

  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public void close() throws IOException {
    // Don't close; file systems are singletons and closing it here could break other users
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (!options.isEnsureAtomic()) {
      return createDirect(path, options);
    }
    return new AtomicFileOutputStream(path, this, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    IOException te = null;
    FileSystem hdfs = getFs();
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        // TODO(chaomin): support creating HDFS files with specified block size and replication.
        OutputStream outputStream = new HdfsUnderFileOutputStream(
            FileSystem.create(hdfs, new Path(path),
            new FsPermission(options.getMode().toShort())));
        if (options.getAcl() != null) {
          setAclEntries(path, options.getAcl().getEntries());
        }
        return outputStream;
      } catch (IOException e) {
        LOG.warn("Attempt count {} : {} ", retryPolicy.getAttemptCount(), e.toString());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return isDirectory(path) && delete(path, options.isRecursive());
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return isFile(path) && delete(path, false);
  }

  @Override
  public boolean exists(String path) throws IOException {
    FileSystem hdfs = getFs();
    return hdfs.exists(new Path(path));
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException {
    return mHdfsAclProvider.getAcl(getFs(), path);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    mHdfsAclProvider.setAclEntries(getFs(), path, aclEntries);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    Path tPath = new Path(path);
    FileSystem hdfs = getFs();
    if (!hdfs.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = hdfs.getFileStatus(tPath);
    return fs.getBlockSize();
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileSystem hdfs = getFs();
    FileStatus fs = hdfs.getFileStatus(tPath);
    return new UfsDirectoryStatus(path, fs.getOwner(), fs.getGroup(),
        fs.getPermission().toShort(), fs.getModificationTime());
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return getFileLocations(path, FileLocationOptions.defaults());
  }

  @Override
  @Nullable
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    // If the user has hinted the underlying storage nodes are not co-located with Alluxio
    // workers, short circuit without querying the locations.
    if (Boolean.valueOf(mUfsConf.get(PropertyKey.UNDERFS_HDFS_REMOTE))) {
      return null;
    }
    FileSystem hdfs = getFs();
    List<String> ret = new ArrayList<>();
    try {
      // The only usage of fileStatus is to get the path in getFileBlockLocations.
      // In HDFS 2, there is an API getFileBlockLocation(Path path, long offset, long len),
      // but in HDFS 1, the only API is
      // getFileBlockLocation(FileStatus stat, long offset, long len).
      // By constructing the file status manually, we can save one RPC call to getFileStatus.
      FileStatus fileStatus = new FileStatus(0L, false, 0, 0L,
          0L, 0L, null, null, null, new Path(path));
      BlockLocation[] bLocations =
          hdfs.getFileBlockLocations(fileStatus, options.getOffset(), 1);
      if (bLocations.length > 0) {
        String[] names = bLocations[0].getHosts();
        Collections.addAll(ret, names);
      }
    } catch (IOException e) {
      LOG.debug("Unable to get file location for {}", path, e);
    }
    return ret;
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileSystem hdfs = getFs();
    FileStatus fs = hdfs.getFileStatus(tPath);
    String contentHash =
        UnderFileSystemUtils.approximateContentHash(fs.getLen(), fs.getModificationTime());
    return new UfsFileStatus(path, contentHash, fs.getLen(), fs.getModificationTime(),
        fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getBlockSize());
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // Ignoring the path given, will give information for entire cluster
    // as Alluxio can load/store data out of entire HDFS cluster
    FileSystem hdfs = getFs();
    long space = -1;
    if (hdfs instanceof DistributedFileSystem) {
      // Note that, getDiskStatus() is an API from Hadoop 1, deprecated by getStatus() from
      // Hadoop 2 and removed in Hadoop 3
      switch (type) {
        case SPACE_TOTAL:
          //#ifdef HADOOP1
          space = ((DistributedFileSystem) hdfs).getDiskStatus().getCapacity();
          //#else
          space = hdfs.getStatus().getCapacity();
          //#endif
          break;
        case SPACE_USED:
          //#ifdef HADOOP1
          space = ((DistributedFileSystem) hdfs).getDiskStatus().getDfsUsed();
          //#else
          space = hdfs.getStatus().getUsed();
          //#endif
          break;
        case SPACE_FREE:
          //#ifdef HADOOP1
          space = ((DistributedFileSystem) hdfs).getDiskStatus().getRemaining();
          //#else
          space = hdfs.getStatus().getRemaining();
          //#endif
          break;
        default:
          throw new IOException("Unknown space type: " + type);
      }
    }
    return space;
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileSystem hdfs = getFs();
    FileStatus fs = hdfs.getFileStatus(tPath);
    if (!fs.isDir()) {
      // Return file status.
      String contentHash =
          UnderFileSystemUtils.approximateContentHash(fs.getLen(), fs.getModificationTime());
      return new UfsFileStatus(path, contentHash, fs.getLen(), fs.getModificationTime(),
          fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getBlockSize());
    }
    // Return directory status.
    return new UfsDirectoryStatus(path, fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(),
        fs.getModificationTime());
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    FileSystem hdfs = getFs();
    return hdfs.isDirectory(new Path(path));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    FileSystem hdfs = getFs();
    return hdfs.isFile(new Path(path));
  }

  @Override
  @Nullable
  public UfsStatus[] listStatus(String path) throws IOException {
    FileStatus[] files = listStatusInternal(path);
    if (files == null) {
      return null;
    }
    UfsStatus[] rtn = new UfsStatus[files.length];
    int i = 0;
    for (FileStatus status : files) {
      // only return the relative path, to keep consistent with java.io.File.list()
      UfsStatus retStatus;
      if (!status.isDir()) {
        String contentHash = UnderFileSystemUtils
            .approximateContentHash(status.getLen(), status.getModificationTime());
        retStatus = new UfsFileStatus(status.getPath().getName(), contentHash, status.getLen(),
            status.getModificationTime(), status.getOwner(), status.getGroup(),
            status.getPermission().toShort(), status.getBlockSize());
      } else {
        retStatus = new UfsDirectoryStatus(status.getPath().getName(), status.getOwner(),
            status.getGroup(), status.getPermission().toShort(), status.getModificationTime());
      }
      rtn[i++] = retStatus;
    }
    return rtn;
  }

  @Override
  public void connectFromMaster(String host) throws IOException {
    if (!mUfsConf.isSet(PropertyKey.MASTER_KEYTAB_KEY_FILE)
        || !mUfsConf.isSet(PropertyKey.MASTER_PRINCIPAL)) {
      return;
    }
    String masterKeytab = mUfsConf.get(PropertyKey.MASTER_KEYTAB_KEY_FILE);
    String masterPrincipal = mUfsConf.get(PropertyKey.MASTER_PRINCIPAL);

    login(PropertyKey.MASTER_KEYTAB_KEY_FILE, masterKeytab, PropertyKey.MASTER_PRINCIPAL,
        masterPrincipal, host);
  }

  @Override
  public void connectFromWorker(String host) throws IOException {
    if (!mUfsConf.isSet(PropertyKey.WORKER_KEYTAB_FILE)
        || !mUfsConf.isSet(PropertyKey.WORKER_PRINCIPAL)) {
      return;
    }
    String workerKeytab = mUfsConf.get(PropertyKey.WORKER_KEYTAB_FILE);
    String workerPrincipal = mUfsConf.get(PropertyKey.WORKER_PRINCIPAL);

    login(PropertyKey.WORKER_KEYTAB_FILE, workerKeytab, PropertyKey.WORKER_PRINCIPAL,
        workerPrincipal, host);
  }

  private void login(PropertyKey keytabFileKey, String keytabFile, PropertyKey principalKey,
      String principal, String hostname) throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set(keytabFileKey.toString(), keytabFile);
    conf.set(principalKey.toString(), principal);
    SecurityUtil.login(conf, keytabFileKey.toString(), principalKey.toString(), hostname);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    IOException te = null;
    FileSystem hdfs = getFs();
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        Path hdfsPath = new Path(path);
        if (hdfs.exists(hdfsPath)) {
          LOG.debug("Trying to create existing directory at {}", path);
          return false;
        }
        // Create directories one by one with explicit permissions to ensure no umask is applied,
        // using mkdirs will apply the permission only to the last directory
        Stack<Path> dirsToMake = new Stack<>();
        dirsToMake.push(hdfsPath);
        Path parent = hdfsPath.getParent();
        while (!hdfs.exists(parent)) {
          dirsToMake.push(parent);
          parent = parent.getParent();
        }
        while (!dirsToMake.empty()) {
          Path dirToMake = dirsToMake.pop();
          if (!FileSystem.mkdirs(hdfs, dirToMake,
              new FsPermission(options.getMode().toShort()))) {
            return false;
          }
          // Set the owner to the Alluxio client user to achieve permission delegation.
          // Alluxio server-side user is required to be a HDFS superuser. If it fails to set owner,
          // proceeds with mkdirs and print out an warning message.
          try {
            setOwner(dirToMake.toString(), options.getOwner(), options.getGroup());
          } catch (IOException e) {
            LOG.warn("Failed to update the ufs dir ownership, default values will be used. " + e);
          }
        }
        return true;
      } catch (IOException e) {
        LOG.warn("{} try to make directory for {} : {}", retryPolicy.getAttemptCount(), path,
            e.toString());
        te = e;
      }
    }
    throw te;
  }

  private boolean isReadLocal(FileSystem fs, Path filePath, OpenOptions options) {
    String localHost = NetworkAddressUtils.getLocalHostName((int) mUfsConf
        .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    BlockLocation[] blockLocations;
    try {
      blockLocations = fs.getFileBlockLocations(filePath,
          options.getOffset(), options.getLength());
      if (blockLocations == null) {
        // no blocks exist
        return true;
      }

      for (BlockLocation loc : blockLocations) {
        if (Arrays.stream(loc.getHosts()).noneMatch(localHost::equals)) {
          return false;
        }
      }
    } catch (IOException e) {
      return true;
    }
    return true;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    IOException te = null;
    FileSystem hdfs = getFs();
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    DistributedFileSystem dfs = null;
    if (hdfs instanceof DistributedFileSystem) {
      dfs = (DistributedFileSystem) hdfs;
    }
    Path filePath = new Path(path);
    boolean remote = options.getPositionShort()
        || mUfsConf.getBoolean(PropertyKey.UNDERFS_HDFS_REMOTE)
        || !isReadLocal(hdfs, filePath, options);
    while (retryPolicy.attempt()) {
      try {
        FSDataInputStream inputStream = hdfs.open(filePath);
        if (remote) {
          LOG.debug("Using pread API to HDFS");
          // pread API instead of seek is more efficient for FSDataInputStream.
          // A seek on FSDataInputStream uses a skip op which is implemented as read + discard
          // and hence ends up reading extra data from the datanode.
          return new HdfsPositionedUnderFileInputStream(inputStream, options.getOffset());
        }
        try {
          inputStream.seek(options.getOffset());
        } catch (IOException e) {
          inputStream.close();
          throw e;
        }
        LOG.debug("Using original API to HDFS");
        return new HdfsUnderFileInputStream(inputStream);
      } catch (IOException e) {
        LOG.warn("{} try to open {} : {}", retryPolicy.getAttemptCount(), path, e.toString());
        te = e;
        if (options.getRecoverFailedOpen() && dfs != null && e.getMessage().toLowerCase()
            .startsWith("cannot obtain block length for")) {
          // This error can occur when an Alluxio journal file was not properly closed by Alluxio.
          // In this scenario, the HDFS lease must be recovered in order for the file to be
          // readable again. The 'recoverLease' API usually needs to be invoked multiple times
          // to complete the lease recovery process.
          try {
            if (dfs.recoverLease(new Path(path))) {
              LOG.warn("HDFS recoverLease-1 success for: {}", path);
            } else {
              // try one more time, after waiting
              CommonUtils.sleepMs(5L * Constants.SECOND_MS);
              if (dfs.recoverLease(new Path(path))) {
                LOG.warn("HDFS recoverLease-2 success for: {}", path);
              } else {
                LOG.warn("HDFS recoverLease: path not closed: {}", path);
              }
            }
          } catch (IOException e1) {
            // ignore exception
            LOG.warn("HDFS recoverLease failed for: {} error: {}", path, e1.getMessage());
          }
        }
      }
    }
    throw te;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    if (!isDirectory(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a file", src, dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!isFile(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a directory", src,
          dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    if (user == null && group == null) {
      return;
    }
    FileSystem hdfs = getFs();
    try {
      FileStatus fileStatus = hdfs.getFileStatus(new Path(path));
      hdfs.setOwner(fileStatus.getPath(), user, group);
    } catch (IOException e) {
      LOG.debug("Exception: ", e);
      if (!mUfsConf.getBoolean(PropertyKey.UNDERFS_ALLOW_SET_OWNER_FAILURE)) {
        LOG.warn("Failed to set owner for {} with user: {}, group: {}: {}. "
            + "Running Alluxio as superuser is required to modify ownership of local files",
            path, user, group, e.toString());
        throw e;
      } else {
        LOG.warn("Failed to set owner for {} with user: {}, group: {}: {}. "
            + "This failure is ignored but may cause permission inconsistency between Alluxio "
            + "and local under file system", path, user, group, e.toString());
      }
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    FileSystem hdfs = getFs();
    try {
      FileStatus fileStatus = hdfs.getFileStatus(new Path(path));
      hdfs.setPermission(fileStatus.getPath(), new FsPermission(mode));
    } catch (IOException e) {
      LOG.warn("Fail to set permission for {} with perm {} : {}", path, mode, e.toString());
      throw e;
    }
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return true;
  }

  @Override
  public boolean supportsActiveSync() {
    return !(mHdfsActiveSyncer instanceof NoopHdfsActiveSyncProvider);
  }

  @Override
  public SyncInfo getActiveSyncInfo() {
    return mHdfsActiveSyncer.getActivitySyncInfo();
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return mHdfsActiveSyncer.startPolling(txId);
  }

  @Override
  public boolean stopActiveSyncPolling() {
    return mHdfsActiveSyncer.stopPolling();
  }

  @Override
  public void startSync(AlluxioURI ufsUri) {
    mHdfsActiveSyncer.startSync(ufsUri);
  }

  @Override
  public void stopSync(AlluxioURI ufsUri) {
    mHdfsActiveSyncer.stopSync(ufsUri);
  }

  /**
   * Delete a file or directory at path.
   *
   * @param path file or directory path
   * @param recursive whether to delete path recursively
   * @return true, if succeed
   */
  private boolean delete(String path, boolean recursive) throws IOException {
    IOException te = null;
    FileSystem hdfs = getFs();
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        return hdfs.delete(new Path(path), recursive);
      } catch (IOException e) {
        LOG.warn("Attempt count {} : {}", retryPolicy.getAttemptCount(), e.toString());
        te = e;
      }
    }
    throw te;
  }

  /**
   * List status for given path. Returns an array of {@link FileStatus} with an entry for each file
   * and directory in the directory denoted by this path.
   *
   * @param path the pathname to list
   * @return {@code null} if the path is not a directory
   */
  @Nullable
  private FileStatus[] listStatusInternal(String path) throws IOException {
    FileStatus[] files;
    FileSystem hdfs = getFs();
    Path thePath = new Path(path);
    try {
      files = hdfs.listStatus(thePath);
    } catch (FileNotFoundException e) {
      return null;
    }
    // Check if path is a file
    if (files != null && files.length == 1
        && files[0].getPath().toUri().getPath().equals(thePath.toUri().getPath())) {
      return null;
    }
    return files;
  }

  /**
   * Rename a file or folder to a file or folder.
   *
   * @param src path of source file or directory
   * @param dst path of destination file or directory
   * @return true if rename succeeds
   */
  private boolean rename(String src, String dst) throws IOException {
    IOException te = null;
    FileSystem hdfs = getFs();
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        return hdfs.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        LOG.warn("{} try to rename {} to {} : {}", retryPolicy.getAttemptCount(), src, dst,
            e.toString());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean isSeekable() {
    return true;
  }

  /**
   * @return the underlying HDFS {@link FileSystem} object
   */
  private FileSystem getFs() throws IOException {
    try {
      // TODO(gpang): handle different users
      return mUserFs.get(HDFS_USER);
    } catch (ExecutionException e) {
      throw new IOException("Failed get FileSystem for " + mUri, e.getCause());
    }
  }
}
