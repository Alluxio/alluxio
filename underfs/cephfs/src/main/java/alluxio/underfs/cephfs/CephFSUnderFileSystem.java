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

package alluxio.underfs.cephfs;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
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
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.ceph.fs.CephFileAlreadyExistsException;
import com.ceph.fs.CephMount;
import com.ceph.fs.CephStat;
import com.ceph.fs.CephStatVFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.util.List;
import java.util.Stack;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * CephFS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class CephFSUnderFileSystem extends ConsistentUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(CephFSUnderFileSystem.class);
  private static final int MAX_TRY = 5;

  private CephMount mMount;
  private static final String CEPH_AUTH_KEY = "key";
  private static final String CEPH_AUTH_KEYFILE = "keyfile";
  private static final String CEPH_AUTH_KEYRING = "keyring";
  private static final String CEPH_MON_HOST = "mon_host";
  private static final String CEPH_CLIENT_MDS_NAMESPACE = "client_mds_namespace";
  private static final String CEPH_CLIENT_MOUNT_UID = "client_mount_uid";
  private static final String CEPH_CLIENT_MOUNT_GID = "client_mount_gid";

  /**
   * Factory method to constructs a new CephFS {@link UnderFileSystem} instance.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop
   * @return a new CephFS {@link UnderFileSystem} instance
   */
  public static CephFSUnderFileSystem createInstance(
      AlluxioURI ufsUri, UnderFileSystemConfiguration conf) throws IOException {
    LOG.info("CephFS URI: {}", ufsUri.toString());

    // Create mount with auth user id
    String userId = null;
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_AUTH_ID)) {
      userId = conf.get(PropertyKey.UNDERFS_CEPHFS_AUTH_ID);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_AUTH_ID, userId);
      if (userId != null && userId.isEmpty()) {
        userId = null;
      }
    }
    CephMount mount = new CephMount(userId);

    // Load a configuration file if specified
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_CONF_FILE)) {
      String confFile = conf.get(PropertyKey.UNDERFS_CEPHFS_CONF_FILE);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_CONF_FILE, confFile);
      if (confFile != null && !confFile.isEmpty()) {
        File file = new File(confFile);
        if (file.exists() && file.isFile()) {
          mount.conf_read_file(confFile);
        }
      }
    }

    // Parse and set Ceph configuration options
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_CONF_OPTS)) {
      String confOpts = conf.get(PropertyKey.UNDERFS_CEPHFS_CONF_OPTS);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_CONF_OPTS, confOpts);
      if (confOpts != null && !confOpts.isEmpty()) {
        String[] options = confOpts.split(";");
        for (String option : options) {
          String[] keyval = option.split("=");
          if (keyval.length != 2) {
            throw new IllegalArgumentException("Invalid Ceph option: " + option);
          }
          String k = keyval[0];
          String v = keyval[1];
          try {
            mount.conf_set(k, v);
          } catch (Exception e) {
            throw new IOException("Error setting Ceph option " + k + " = " + v);
          }
        }
      }
    }

    // Set auth key
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_AUTH_KEY)) {
      String key = conf.get(PropertyKey.UNDERFS_CEPHFS_AUTH_KEY);
      if (key != null && !key.isEmpty()) {
        mount.conf_set(CEPH_AUTH_KEY, key);
      }
    }

    // Set auth keyfile
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_AUTH_KEYFILE)) {
      String keyfile = conf.get(PropertyKey.UNDERFS_CEPHFS_AUTH_KEYFILE);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_AUTH_KEYFILE, keyfile);
      if (keyfile != null && !keyfile.isEmpty()) {
        mount.conf_set(CEPH_AUTH_KEYFILE, keyfile);
      }
    }

    // Set auth keyring
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_AUTH_KEYRING)) {
      String keyring = conf.get(PropertyKey.UNDERFS_CEPHFS_AUTH_KEYRING);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_AUTH_KEYRING, keyring);
      if (keyring != null && !keyring.isEmpty()) {
        mount.conf_set(CEPH_AUTH_KEYRING, keyring);
      }
    }

    // Set monitor hosts
    String monHost = ufsUri.getAuthority().toString();
    if (monHost == null || monHost.isEmpty()) {
      if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_MON_HOST)) {
        monHost = conf.get(PropertyKey.UNDERFS_CEPHFS_MON_HOST);
        LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_MON_HOST, monHost);
      }
    }

    if (monHost != null && !monHost.isEmpty()) {
      mount.conf_set(CEPH_MON_HOST, monHost);
    }

    // Set filesystem to mount
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_MDS_NAMESPACE)) {
      String namespace = conf.get(PropertyKey.UNDERFS_CEPHFS_MDS_NAMESPACE);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_MDS_NAMESPACE, namespace);
      if (namespace != null && !namespace.isEmpty()) {
        mount.conf_set(CEPH_CLIENT_MDS_NAMESPACE, namespace);
      }
    }

    // Set uid/gid to mount
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_MOUNT_UID)) {
      String uid = conf.get(PropertyKey.UNDERFS_CEPHFS_MOUNT_UID);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_MOUNT_UID, uid);
      if (uid != null && !uid.isEmpty()) {
        mount.conf_set(CEPH_CLIENT_MOUNT_UID, uid);
      }
    }

    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_MOUNT_GID)) {
      String gid = conf.get(PropertyKey.UNDERFS_CEPHFS_MOUNT_GID);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_MOUNT_GID, gid);
      if (gid != null && !gid.isEmpty()) {
        mount.conf_set(CEPH_CLIENT_MOUNT_GID, gid);
      }
    }

    // Actually mount the file system
    String root = null;
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_MOUNT_POINT)) {
      root = conf.get(PropertyKey.UNDERFS_CEPHFS_MOUNT_POINT);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_MOUNT_POINT, root);
      if (root != null && root.isEmpty()) {
        root = null;
      }
    }
    mount.mount(root);

    // Allow reads from replica objects
    if (conf.isSetByUser(PropertyKey.UNDERFS_CEPHFS_LOCALIZE_READS)) {
      String localizeReads = conf.get(PropertyKey.UNDERFS_CEPHFS_LOCALIZE_READS);
      LOG.info("CephFS config: {} = {}", PropertyKey.UNDERFS_CEPHFS_LOCALIZE_READS, localizeReads);
      if (localizeReads != null && !localizeReads.isEmpty()) {
        boolean bLocalize = Boolean.parseBoolean(localizeReads);
        mount.localize_reads(bLocalize);
      }
    }

    return new CephFSUnderFileSystem(ufsUri, mount, conf);
  }

  /**
   * Constructs a new CephFS {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param mount CephMount instance
   * @param conf the configuration for this UFS
   */
  public CephFSUnderFileSystem(AlluxioURI ufsUri, CephMount mount,
      UnderFileSystemConfiguration conf) {
    super(ufsUri, conf);
    mMount = mount;
  }

  @Override
  public String getUnderFSType() {
    return "cephfs";
  }

  @Override
  public void close() throws IOException {
    if (null != mMount) {
      mMount.unmount();
    }
    mMount = null;
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (!options.isEnsureAtomic()) {
      return createDirect(path, options);
    }
    return new AtomicFileOutputStream(path, this, options);
  }

  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    path = stripPath(path);
    String parentPath;
    try {
      parentPath = PathUtils.getParent(path);
    } catch (InvalidPathException e) {
      throw new IOException("Invalid path " + path, e);
    }

    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        //  support creating CephFS files with specified block size and replication.
        if (options.getCreateParent()) {
          if (mkdirs(parentPath, MkdirsOptions.defaults(mUfsConf)) && !isDirectory(parentPath)) {
            throw new IOException(ExceptionMessage.PARENT_CREATION_FAILED.getMessage(path));
          }
        }

        int flags = CephMount.O_WRONLY | CephMount.O_CREAT | CephMount.O_TRUNC;
        short mode = options.getMode().toShort();

        int fd = openInternal(path, flags, mode);
        return new CephOutputStream(mMount, fd);
      } catch (IOException e) {
        LOG.warn("Retry count {} : {}", retryPolicy.getAttemptCount(), e.toString());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    path = stripPath(path);
    if (isDirectory(path)) {
      IOException te = null;
      RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
      while (retryPolicy.attempt()) {
        try {
          return deleteInternal(path, options.isRecursive());
        } catch (IOException e) {
          LOG.warn("Retry count {} : {}", retryPolicy.getAttemptCount(), e.toString());
          te = e;
        }
      }
      throw te;
    }
    return false;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    path = stripPath(path);
    if (isFile(path)) {
      IOException te = null;
      RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
      while (retryPolicy.attempt()) {
        try {
          return deleteInternal(path, false);
        } catch (IOException e) {
          LOG.warn("Retry count {} : {}", retryPolicy.getAttemptCount(), e.toString());
          te = e;
        }
      }
      throw te;
    }
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    path = stripPath(path);
    try {
      CephStat stat = new CephStat();
      lstat(path, stat);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return stat.blksize;
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return new UfsDirectoryStatus(path, "", "", (short) stat.mode);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);
    if (stat.isFile()) {
      return getFileStatus(path);
    } else if (stat.isDir()) {
      return getDirectoryStatus(path);
    }

    throw new IOException("Failed to getStatus: " + path);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return null;
  }

  @Override
  @Nullable
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return null;
  }

  /**
   * Gets stat information on a file. This does not fill owner or group, as
   * Ceph's support for these is a bit different.
   *
   * @param path The path to stat
   * @return FileStatus object containing the stat information
   * @throws FileNotFoundException if the path could not be resolved
   */
  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);
    String contentHash =
        UnderFileSystemUtils.approximateContentHash(stat.size, stat.m_time);

    return new UfsFileStatus(path, contentHash, stat.size, stat.m_time,
        "", "", (short) stat.mode);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    path = stripPath(path);
    CephStatVFS stat = new CephStatVFS();
    statfs(path, stat);

    // Ignoring the path given, will give information for entire cluster
    // as Alluxio can load/store data out of entire CephFS cluster
    switch (type) {
      case SPACE_TOTAL:
        return stat.bsize * stat.blocks;
      case SPACE_USED:
        return stat.bsize * (stat.blocks - stat.bavail);
      case SPACE_FREE:
        return stat.bsize * stat.bavail;
      default:
        throw new IOException("Unknown space type: " + type);
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    path = stripPath(path);
    try {
      CephStat stat = new CephStat();
      lstat(path, stat);
      return stat.isDir();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
    path = stripPath(path);
    try {
      CephStat stat = new CephStat();
      lstat(path, stat);
      return stat.isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Each string is a name rather than a complete path.
   *
   * @param path the path to list
   * @return An array with the statuses of the files and directories in the directory
   * denoted by this path. The array will be empty if the directory is empty. Returns
   * null if this path does not denote a directory
   */
  @Override
  @Nullable
  public UfsStatus[] listStatus(String path) throws IOException {
    path = stripPath(path);
    String[] lst = listDirectory(path);
    if (lst != null) {
      UfsStatus[] status = new UfsStatus[lst.length];

      for (int i = 0; i < status.length; i++) {
        CephStat stat = new CephStat();
        lstat(PathUtils.concatPath(path, lst[i]), stat);

        if (!stat.isDir()) {
          String contentHash =
                 UnderFileSystemUtils.approximateContentHash(stat.size, stat.m_time);
          status[i] = new UfsFileStatus(lst[i], contentHash, stat.size, stat.m_time,
              "", "", (short) stat.mode);
        } else {
          status[i] = new UfsDirectoryStatus(lst[i], "", "",
              (short) stat.mode);
        }
      }
      return status;
    }
    return null;
  }

  @Override
  public void connectFromMaster(String host) throws IOException {
    // no-op
  }

  @Override
  public void connectFromWorker(String host) throws IOException {
    // no-op
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    path = stripPath(path);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        if (exists(path)) {
          LOG.debug("Trying to create existing directory at {}", path);
          return false;
        }
        String parent = getParentPath(path);
        if (!options.getCreateParent() && !isDirectory(parent)) {
          return false;
        }
        // Create directories one by one with explicit permissions to ensure no umask is applied,
        // using mkdirs will apply the permission only to the last directory
        Stack<String> dirsToMake = new Stack<>();
        dirsToMake.push(path);
        while (!exists(parent)) {
          dirsToMake.push(parent);
          parent = getParentPath(parent);
        }
        while (!dirsToMake.empty()) {
          String dirToMake = dirsToMake.pop();
          try {
            mMount.mkdirs(dirToMake, options.getMode().toShort());
          } catch (CephFileAlreadyExistsException e) {
            // can be ignored safely
          }
          try {
            setOwner(dirToMake, options.getOwner(), options.getGroup());
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

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    path = stripPath(path);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        int mode = CreateOptions.defaults(mUfsConf).getMode().toShort();
        int fd = openInternal(path, CephMount.O_RDONLY, mode);
        CephStat stat = new CephStat();
        mMount.fstat(fd, stat);
        CephInputStream inputStream = new CephInputStream(mMount, fd, stat.size);
        try {
          inputStream.seek(options.getOffset());
        } catch (IOException e) {
          inputStream.close();
          throw e;
        }
        return inputStream;
      } catch (IOException e) {
        LOG.warn("{} try to open {} : {}", retryPolicy.getAttemptCount(), path, e.toString());
        te = e;
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
  public boolean renameRenamableDirectory(String src, String dst) throws IOException {
    return renameDirectory(src, dst);
  }

  @Override
  public boolean renameRenamableFile(String src, String dst) throws IOException {
    return renameFile(src, dst);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    // no-op, Ceph's support for these is a bit different
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    path = stripPath(path);
    mMount.chmod(path, mode);
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  /**
   * To strip the path.
   *
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    return new AlluxioURI(path).getPath();
  }

  private String getParentPath(String path) throws IOException {
    try {
      return PathUtils.getParent(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  private String getFileName(String path) throws IOException {
    try {
      String parent = PathUtils.getParent(path);
      return PathUtils.subtractPaths(path, parent);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  private String[] listDirectory(String path) throws IOException {
    CephStat stat = new CephStat();
    try {
      mMount.lstat(path, stat);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (!stat.isDir()) {
      return null;
    }
    return mMount.listdir(path);
  }

  private int openInternal(String path, int flags, int mode) throws IOException {
    int fd = mMount.open(path, flags, mode);
    CephStat stat = new CephStat();
    mMount.fstat(fd, stat);
    if (stat.isDir()) {
      mMount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  private boolean deleteInternal(String path, boolean recursive) throws IOException {
    CephStat stat = new CephStat();
    try {
      lstat(path, stat);
    } catch (FileNotFoundException e) {
      return false;
    }

    // we're done if it's a file
    if (stat.isFile()) {
      mMount.unlink(path);
      return true;
    }

    // get directory contents
    String[] lst = listDirectory(path);
    if (lst == null) {
      return false;
    }

    if (!recursive && lst.length > 0) {
      throw new IOException("Directory " + path + " is not empty.");
    }

    for (String fname : lst) {
      String fullPath = PathUtils.concatPath(path, fname);
      if (!deleteInternal(fullPath, recursive)) {
        return false;
      }
    }

    mMount.rmdir(path);
    return true;
  }

  /**
   * Rename a file or folder to a file or folder.
   *
   * @param src path of source file or directory
   * @param dst path of destination file or directory
   * @return true if rename succeeds
   */
  private boolean rename(String src, String dst) throws IOException {
    src = stripPath(src);
    dst = stripPath(dst);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        try {
          CephStat stat = new CephStat();
          lstat(dst, stat);
          if (stat.isDir()) {
            String fileName = getFileName(src);
            mMount.rename(src, PathUtils.concatPath(dst, fileName));
            return true;
          }
          return false;
        } catch (FileNotFoundException e) {
          // can be ignored safely
        }

        mMount.rename(src, dst);
        return true;
      } catch (IOException e) {
        LOG.warn("{} try to rename {} to {} : {}", retryPolicy.getAttemptCount(), src, dst,
            e.toString());
        te = e;
      }
    }
    throw te;
  }

  private void lstat(String path, CephStat stat) throws IOException {
    mMount.lstat(path, stat);
  }

  private void statfs(String path, CephStatVFS stat) throws IOException {
    mMount.statfs(path, stat);
  }
}

