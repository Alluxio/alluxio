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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authorization.Permission;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * HDFS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class HdfsUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MAX_TRY = 5;
  // TODO(hy): Add a sticky bit and narrow down the permission in hadoop 2.
  private static final FsPermission PERMISSION = new FsPermission((short) 0777)
      .applyUMask(FsPermission.createImmutable((short) 0000));

  private FileSystem mFileSystem;

  /**
   * Constructs a new HDFS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop
   */
  public HdfsUnderFileSystem(AlluxioURI uri, Object conf) {
    super(uri);
    final String ufsPrefix = uri.toString();
    final org.apache.hadoop.conf.Configuration hadoopConf;
    if (conf != null && conf instanceof org.apache.hadoop.conf.Configuration) {
      hadoopConf = (org.apache.hadoop.conf.Configuration) conf;
    } else {
      hadoopConf = new org.apache.hadoop.conf.Configuration();
    }
    prepareConfiguration(ufsPrefix, hadoopConf);
    hadoopConf.addResource(
        new Path(hadoopConf.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION.toString())));
    HdfsUnderFileSystemUtils.addS3Credentials(hadoopConf);

    Path path = new Path(ufsPrefix);
    try {
      mFileSystem = path.getFileSystem(hadoopConf);
    } catch (IOException e) {
      LOG.error("Exception thrown when trying to get FileSystem for {}", ufsPrefix, e);
      throw Throwables.propagate(e);
    }
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
   * @param path file system path
   * @param hadoopConf Hadoop configuration
   */
  protected void prepareConfiguration(String path,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    // On Hadoop 2.x this is strictly unnecessary since it uses ServiceLoader to automatically
    // discover available file system implementations. However this configuration setting is
    // required for earlier Hadoop versions plus it is still honoured as an override even in 2.x so
    // if present propagate it to the Hadoop configuration
    String ufsHdfsImpl = Configuration.get(PropertyKey.UNDERFS_HDFS_IMPL);
    if (!StringUtils.isEmpty(ufsHdfsImpl)) {
      hadoopConf.set("fs.hdfs.impl", ufsHdfsImpl);
    }

    // Disable hdfs client caching so that input configuration is respected. Configurable from
    // system property
    hadoopConf.set("fs.hdfs.impl.disable.cache",
        System.getProperty("fs.hdfs.impl.disable.cache", "true"));

    HdfsUnderFileSystemUtils.addKey(hadoopConf, PropertyKey.UNDERFS_HDFS_CONFIGURATION);
  }

  @Override
  public void close() throws IOException {
    // Don't close; file systems are singletons and closing it here could break other users
  }

  @Override
  public FSDataOutputStream create(String path) throws IOException {
    return create(path, new CreateOptions());
  }

  @Override
  public FSDataOutputStream create(String path, CreateOptions options)
      throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    Permission perm = options.getPermission();
    while (retryPolicy.attemptRetry()) {
      try {
        LOG.debug("Creating HDFS file at {} with perm {}", path, perm.toString());
        // TODO(chaomin): support creating HDFS files with specified block size and replication.
        return FileSystem.create(mFileSystem, new Path(path),
            new FsPermission(perm.getMode().toShort()));
      } catch (IOException e) {
        LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("deleting {} {}", path, recursive);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.delete(new Path(path), recursive);
      } catch (IOException e) {
        LOG.error("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean fileExists(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.isFile(new Path(path));
      } catch (IOException e) {
        LOG.error("{} try to check if {} exists : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean directoryExists(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.isDirectory(new Path(path));
      } catch (IOException e) {
        LOG.error("{} try to check if {} exists : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean fileOrFolderExists(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.exists(new Path(path));
      } catch (IOException e) {
        LOG.error("{} try to check if {} exists : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
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
  public Object getConf() {
    return mFileSystem.getConf();
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return getFileLocations(path, 0);
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    List<String> ret = new ArrayList<>();
    try {
      FileStatus fStatus = mFileSystem.getFileStatus(new Path(path));
      BlockLocation[] bLocations = mFileSystem.getFileBlockLocations(fStatus, offset, 1);
      if (bLocations.length > 0) {
        String[] names = bLocations[0].getHosts();
        Collections.addAll(ret, names);
      }
    } catch (IOException e) {
      LOG.error("Unable to get file location for {}", path, e);
    }
    return ret;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    Path tPath = new Path(path);
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        FileStatus fs = mFileSystem.getFileStatus(tPath);
        return fs.getLen();
      } catch (IOException e) {
        LOG.error("{} try to get file size for {} : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
      }
    }
    return -1;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFileSystem.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return fs.getModificationTime();
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
          throw new IOException("Unknown getSpace parameter: " + type);
      }
    }
    return -1;
  }

  @Override
  public String[] list(String path) throws IOException {
    FileStatus[] files;
    try {
      files = mFileSystem.listStatus(new Path(path));
    } catch (FileNotFoundException e) {
      return null;
    }
    if (files != null && !fileExists(path)) {
      String[] rtn = new String[files.length];
      int i = 0;
      for (FileStatus status : files) {
        // only return the relative path, to keep consistent with java.io.File.list()
        rtn[i++] =  status.getPath().getName();
      }
      return rtn;
    } else {
      return null;
    }
  }

  @Override
  public void connectFromMaster(String host) throws IOException {
    if (!Configuration.containsKey(PropertyKey.MASTER_KEYTAB_KEY_FILE)
        || !Configuration.containsKey(PropertyKey.MASTER_PRINCIPAL)) {
      return;
    }
    String masterKeytab = Configuration.get(PropertyKey.MASTER_KEYTAB_KEY_FILE);
    String masterPrincipal = Configuration.get(PropertyKey.MASTER_PRINCIPAL);

    login(PropertyKey.MASTER_KEYTAB_KEY_FILE, masterKeytab, PropertyKey.MASTER_PRINCIPAL,
        masterPrincipal, host);
  }

  @Override
  public void connectFromWorker(String host) throws IOException {
    if (!Configuration.containsKey(PropertyKey.WORKER_KEYTAB_FILE)
        || !Configuration.containsKey(PropertyKey.WORKER_PRINCIPAL)) {
      return;
    }
    String workerKeytab = Configuration.get(PropertyKey.WORKER_KEYTAB_FILE);
    String workerPrincipal = Configuration.get(PropertyKey.WORKER_PRINCIPAL);

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
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, new MkdirsOptions().setCreateParent(createParent));
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
          if (!FileSystem.mkdirs(mFileSystem, dirsToMake.pop(),
              new FsPermission(options.getPermission().getMode().toShort()))) {
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
  public FSDataInputStream open(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.open(new Path(path));
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
    if (!fileOrFolderExists(src)) {
      LOG.error("File {} does not exist. Therefore rename to {} failed.", src, dst);
      return false;
    }

    if (fileOrFolderExists(dst)) {
      LOG.error("File {} does exist. Therefore rename from {} failed.", dst, src);
      return false;
    }

    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        LOG.error("{} try to rename {} to {} : {}", retryPolicy.getRetryCount(), src, dst,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public void setConf(Object conf) {
    mFileSystem.setConf((org.apache.hadoop.conf.Configuration) conf);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      LOG.info("Changing file '{}' user from: {} to {}, group from: {} to {}", fileStatus.getPath(),
          fileStatus.getOwner(), user, fileStatus.getGroup(), group);
      mFileSystem.setOwner(fileStatus.getPath(), user, group);
    } catch (IOException e) {
      LOG.error("Fail to set owner for {} with user: {}, group: {}", path, user, group, e);
      LOG.warn("In order for Alluxio to create HDFS files with the correct user and groups, "
          + "Alluxio should be added to the HDFS superusers.");
      throw e;
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      LOG.info("Changing file '{}' permissions from: {} to {}", fileStatus.getPath(),
          fileStatus.getPermission(), mode);
      mFileSystem.setPermission(fileStatus.getPath(), new FsPermission(mode));
    } catch (IOException e) {
      LOG.error("Fail to set permission for {} with perm {}", path, mode, e);
      throw e;
    }
  }

  @Override
  public String getOwner(String path) throws IOException {
    try {
      return mFileSystem.getFileStatus(new Path(path)).getOwner();
    } catch (IOException e) {
      LOG.error("Fail to get owner for {} ", path, e);
      throw e;
    }
  }

  @Override
  public String getGroup(String path) throws IOException {
    try {
      return mFileSystem.getFileStatus(new Path(path)).getGroup();
    } catch (IOException e) {
      LOG.error("Fail to get group for {} ", path, e);
      throw e;
    }
  }

  @Override
  public short getMode(String path) throws IOException {
    try {
      return mFileSystem.getFileStatus(new Path(path)).getPermission().toShort();
    } catch (IOException e) {
      LOG.error("Fail to get permission for {} ", path, e);
      throw e;
    }
  }

}
