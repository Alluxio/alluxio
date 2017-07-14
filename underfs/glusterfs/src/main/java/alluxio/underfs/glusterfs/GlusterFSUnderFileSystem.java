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

package alluxio.underfs.glusterfs;

import alluxio.AlluxioURI;
import alluxio.PropertyKey;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.BaseUnderFileSystem;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An {@link UnderFileSystem} uses the Gluster FS.
 * <p>
 * Currently this implementation simply manages the extra configuration setup necessary to connect
 * to Gluster FS.
 * </p>
 */
@ThreadSafe
public final class GlusterFSUnderFileSystem extends BaseUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(GlusterFSUnderFileSystem.class);
  private static final int MAX_TRY = 5;

  private FileSystem mFileSystem;
  private UnderFileSystemConfiguration mUfsConf;

  /**
   * Constant for the Gluster FS URI scheme.
   */
  public static final String SCHEME = "glusterfs://";

  /**
   * Prepares the configuration for this Gluster FS as an HDFS configuration.
   *
   * @param conf the configuration for this UFS
   * @return the created configuration
   */
  public static Configuration createConfiguration(UnderFileSystemConfiguration conf) {
    Configuration glusterFsConf = new Configuration();
    // Configure for Gluster FS
    glusterFsConf.set("fs.glusterfs.impl", conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_IMPL));
    glusterFsConf
        .set("mapred.system.dir", conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_MR_DIR));
    glusterFsConf
        .set("fs.glusterfs.volumes", conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES));
    glusterFsConf.set(
        "fs.glusterfs.volume.fuse." + conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES),
        conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_MOUNTS));
    return glusterFsConf;
  }

  /**
   * Factory method to construct a new Gluster FS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return a new Gluster FS {@link UnderFileSystem} instance
   */
  public static GlusterFSUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    Configuration glusterFsConf = createConfiguration(conf);
    return new GlusterFSUnderFileSystem(uri, conf, glusterFsConf);
  }

  /**
   * Constructs a new Gluster FS {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param glusterFsConf the configuration for this Gluster FS
   */
  private GlusterFSUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      Configuration glusterFsConf) {
    super(ufsUri, conf);
    mUfsConf = conf;
    Path path = new Path(ufsUri.toString());
    try {
      mFileSystem = path.getFileSystem(glusterFsConf);
    } catch (IOException e) {
      LOG.warn("Exception thrown when trying to get FileSystem for {} : {}", ufsUri,
          e.getMessage());
      throw new RuntimeException("Failed to create Hadoop FileSystem", e);
    }
  }

  @Override
  public String getUnderFSType() {
    return "glusterfs";
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
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        // TODO(chaomin): support creating HDFS files with specified block size and replication.
        return FileSystem.create(mFileSystem, new Path(path),
            new FsPermission(options.getMode().toShort()));
      } catch (IOException e) {
        LOG.warn("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage());
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
    return mFileSystem.exists(new Path(path));
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFileSystem.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return fs.getBlockSize();
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return new UfsDirectoryStatus(path, fs.getOwner(), fs.getGroup(), fs.getPermission().toShort());
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return getFileLocations(path, FileLocationOptions.defaults());
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    // If the user has hinted the underlying storage nodes are not co-located with Alluxio
    // workers, short circuit without querying the locations
    if (Boolean.valueOf(mUfsConf.getValue(PropertyKey.UNDERFS_HDFS_REMOTE))) {
      return null;
    }
    List<String> ret = new ArrayList<>();
    try {
      FileStatus fStatus = mFileSystem.getFileStatus(new Path(path));
      BlockLocation[] bLocations =
          mFileSystem.getFileBlockLocations(fStatus, options.getOffset(), 1);
      if (bLocations.length > 0) {
        String[] names = bLocations[0].getHosts();
        Collections.addAll(ret, names);
      }
    } catch (IOException e) {
      LOG.warn("Unable to get file location for {} : {}", path, e.getMessage());
    }
    return ret;
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return new UfsFileStatus(path, fs.getLen(), fs.getModificationTime(), fs.getOwner(),
        fs.getGroup(), fs.getPermission().toShort());
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // Ignoring the path given, will give information for entire cluster
    // as Alluxio can load/store data out of entire HDFS cluster
    if (mFileSystem instanceof DistributedFileSystem) {
      switch (type) {
        case SPACE_TOTAL:
          // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
          // FileSystem.getStatus().getCapacity() will be the new one.
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getCapacity();
        case SPACE_USED:
          // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
          // FileSystem.getStatus().getUsed() will be the new one.
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getDfsUsed();
        case SPACE_FREE:
          // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
          // FileSystem.getStatus().getRemaining() will be the new one.
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getRemaining();
        default:
          throw new IOException("Unknown space type: " + type);
      }
    }
    return -1;
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mFileSystem.isDirectory(new Path(path));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mFileSystem.isFile(new Path(path));
  }

  @Override
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
      if (status.isDirectory()) {
        retStatus = new UfsFileStatus(status.getPath().getName(), status.getLen(),
            status.getModificationTime(), status.getOwner(), status.getGroup(),
            status.getPermission().toShort());
      } else {
        retStatus = new UfsDirectoryStatus(status.getPath().getName(), status.getOwner(),
            status.getGroup(), status.getPermission().toShort());
      }
      rtn[i++] = retStatus;
    }
    return rtn;
  }

  @Override
  public void connectFromMaster(String host) throws IOException {
    if (!mUfsConf.containsKey(PropertyKey.MASTER_KEYTAB_KEY_FILE)
        || !mUfsConf.containsKey(PropertyKey.MASTER_PRINCIPAL)) {
      return;
    }
    String masterKeytab = mUfsConf.getValue(PropertyKey.MASTER_KEYTAB_KEY_FILE);
    String masterPrincipal = mUfsConf.getValue(PropertyKey.MASTER_PRINCIPAL);

    login(PropertyKey.MASTER_KEYTAB_KEY_FILE, masterKeytab, PropertyKey.MASTER_PRINCIPAL,
        masterPrincipal, host);
  }

  @Override
  public void connectFromWorker(String host) throws IOException {
    if (!mUfsConf.containsKey(PropertyKey.WORKER_KEYTAB_FILE)
        || !mUfsConf.containsKey(PropertyKey.WORKER_PRINCIPAL)) {
      return;
    }
    String workerKeytab = mUfsConf.getValue(PropertyKey.WORKER_KEYTAB_FILE);
    String workerPrincipal = mUfsConf.getValue(PropertyKey.WORKER_PRINCIPAL);

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
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        Path hdfsPath = new Path(path);
        if (mFileSystem.exists(hdfsPath)) {
          LOG.debug("Trying to create existing directory at {}", path);
          return false;
        }
        // Create directories one by one with explicit permissions to ensure no umask is applied,
        // using mkdirs will apply the permission only to the last directory
        Stack<Path> dirsToMake = new Stack<>();
        dirsToMake.push(hdfsPath);
        Path parent = hdfsPath.getParent();
        while (!mFileSystem.exists(parent)) {
          dirsToMake.push(parent);
          parent = parent.getParent();
        }
        while (!dirsToMake.empty()) {
          Path dirToMake = dirsToMake.pop();
          if (!FileSystem.mkdirs(mFileSystem, dirToMake,
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
        LOG.warn("{} try to make directory for {} : {}", retryPolicy.getRetryCount(), path,
            e.getMessage());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        FSDataInputStream inputStream = mFileSystem.open(new Path(path));
        try {
          inputStream.seek(options.getOffset());
        } catch (IOException e) {
          inputStream.close();
          throw e;
        }
        return inputStream;
      } catch (IOException e) {
        LOG.warn("{} try to open {} : {}", retryPolicy.getRetryCount(), path, e.getMessage());
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
  public void setOwner(String path, String user, String group) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      mFileSystem.setOwner(fileStatus.getPath(), user, group);
    } catch (IOException e) {
      LOG.warn("Failed to set owner for {} with user: {}, group: {}", path, user, group);
      LOG.debug("Exception : ", e);
      LOG.warn("In order for Alluxio to modify ownership of local files, "
          + "Alluxio should be the local file system superuser.");
      if (!Boolean.valueOf(mUfsConf.getValue(PropertyKey.UNDERFS_ALLOW_SET_OWNER_FAILURE))) {
        throw e;
      } else {
        LOG.warn("Failure is ignored, which may cause permission inconsistency between "
            + "Alluxio and HDFS.");
      }
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      mFileSystem.setPermission(fileStatus.getPath(), new FsPermission(mode));
    } catch (IOException e) {
      LOG.warn("Fail to set permission for {} with perm {} : {}", path, mode, e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean supportsFlush() {
    return true;
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
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.delete(new Path(path), recursive);
      } catch (IOException e) {
        LOG.warn("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage());
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
  private FileStatus[] listStatusInternal(String path) throws IOException {
    FileStatus[] files;
    try {
      files = mFileSystem.listStatus(new Path(path));
    } catch (FileNotFoundException e) {
      return null;
    }
    // Check if path is a file
    if (files != null && files.length == 1 && files[0].getPath().toString().equals(path)) {
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
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        LOG.warn("{} try to rename {} to {} : {}", retryPolicy.getRetryCount(), src, dst,
            e.getMessage());
        te = e;
      }
    }
    throw te;
  }
}
