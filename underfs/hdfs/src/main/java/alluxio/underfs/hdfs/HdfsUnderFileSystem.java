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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystem;

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

  private final FileSystem mFileSystem;
  private final String mUfsPrefix;

  /**
   * Constructs a new HDFS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param configuration the configuration for Alluxio
   * @param conf the configuration for Hadoop
   */
  public HdfsUnderFileSystem(AlluxioURI uri, Configuration configuration, Object conf) {
    super(uri, configuration);
    mUfsPrefix = uri.toString();
    org.apache.hadoop.conf.Configuration tConf;
    if (conf != null && conf instanceof org.apache.hadoop.conf.Configuration) {
      tConf = (org.apache.hadoop.conf.Configuration) conf;
    } else {
      tConf = new org.apache.hadoop.conf.Configuration();
    }
    prepareConfiguration(mUfsPrefix, configuration, tConf);
    tConf.addResource(new Path(tConf.get(Constants.UNDERFS_HDFS_CONFIGURATION)));
    HdfsUnderFileSystemUtils.addS3Credentials(tConf);

    Path path = new Path(mUfsPrefix);
    try {
      mFileSystem = path.getFileSystem(tConf);
    } catch (IOException e) {
      LOG.error("Exception thrown when trying to get FileSystem for {}", mUfsPrefix, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.HDFS;
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
   * @param conf Alluxio Configuration
   * @param hadoopConf Hadoop configuration
   */
  protected void prepareConfiguration(String path, Configuration conf,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    // On Hadoop 2.x this is strictly unnecessary since it uses ServiceLoader to automatically
    // discover available file system implementations. However this configuration setting is
    // required for earlier Hadoop versions plus it is still honoured as an override even in 2.x so
    // if present propagate it to the Hadoop configuration
    String ufsHdfsImpl = mConfiguration.get(Constants.UNDERFS_HDFS_IMPL);
    if (!StringUtils.isEmpty(ufsHdfsImpl)) {
      hadoopConf.set("fs.hdfs.impl", ufsHdfsImpl);
    }

    // To disable the instance cache for hdfs client, otherwise it causes the
    // FileSystem closed exception. Being configurable for unit/integration
    // test only, and not expose to the end-user currently.
    hadoopConf.set("fs.hdfs.impl.disable.cache",
        System.getProperty("fs.hdfs.impl.disable.cache", "false"));

    HdfsUnderFileSystemUtils.addKey(hadoopConf, conf, Constants.UNDERFS_HDFS_CONFIGURATION);
  }

  @Override
  public void close() throws IOException {
    // Don't close; file systems are singletons and closing it here could break other users
  }

  @Override
  public FSDataOutputStream create(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        LOG.debug("Creating HDFS file at {}", path);
        return FileSystem.create(mFileSystem, new Path(path), PERMISSION);
      } catch (IOException e) {
        LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  /**
   * Creates a new file.
   *
   * @param path the path
   * @param blockSizeByte the size of the block in bytes; should be a multiple of 512
   * @return a {@code FSDataOutputStream} object
   * @throws IOException when a non-Alluxio related exception occurs
   */
  @Override
  public FSDataOutputStream create(String path, int blockSizeByte) throws IOException {
    // TODO(hy): Fix this.
    // return create(path, (short) Math.min(3, mFileSystem.getDefaultReplication()),
    // blockSizeBytes);
    return create(path);
  }

  @Override
  public FSDataOutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    // TODO(hy): Fix this.
    // return create(path, (short) Math.min(3, mFileSystem.getDefaultReplication()),
    // blockSizeBytes);
    return create(path);
    // LOG.info("{} {} {}", path, replication, blockSizeBytes);
    // IOException te = null;
    // int cnt = 0;
    // while (cnt < MAX_TRY) {
    // try {
    // return mFileSystem.create(new Path(path), true, 4096, replication, blockSizeBytes);
    // } catch (IOException e) {
    // cnt++;
    // LOG.error("{} : {}", cnt, e.getMessage(), e);
    // te = e;
    // continue;
    // }
    // }
    // throw te;
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
  public boolean exists(String path) throws IOException {
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
    List<String> ret = new ArrayList<String>();
    try {
      FileStatus fStatus = mFileSystem.getFileStatus(new Path(path));
      BlockLocation[] bLocations = mFileSystem.getFileBlockLocations(fStatus, offset, 1);
      if (bLocations.length > 0) {
        String[] names = bLocations[0].getNames();
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
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getCapacity();
        case SPACE_USED:
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getDfsUsed();
        case SPACE_FREE:
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getRemaining();
        default:
          throw new IOException("Unknown getSpace parameter: " + type);
      }
    }
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mFileSystem.isFile(new Path(path));
  }

  @Override
  public String[] list(String path) throws IOException {
    FileStatus[] files;
    try {
      files = mFileSystem.listStatus(new Path(path));
    } catch (FileNotFoundException e) {
      return null;
    }
    if (files != null && !isFile(path)) {
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
  public void connectFromMaster(Configuration conf, String host) throws IOException {
    if (!conf.containsKey(Constants.MASTER_KEYTAB_KEY)
        || !conf.containsKey(Constants.MASTER_PRINCIPAL_KEY)) {
      return;
    }
    String masterKeytab = conf.get(Constants.MASTER_KEYTAB_KEY);
    String masterPrincipal = conf.get(Constants.MASTER_PRINCIPAL_KEY);

    login(Constants.MASTER_KEYTAB_KEY, masterKeytab, Constants.MASTER_PRINCIPAL_KEY,
        masterPrincipal, host);
  }

  @Override
  public void connectFromWorker(Configuration conf, String host) throws IOException {
    if (!conf.containsKey(Constants.WORKER_KEYTAB_KEY)
        || !conf.containsKey(Constants.WORKER_PRINCIPAL_KEY)) {
      return;
    }
    String workerKeytab = conf.get(Constants.WORKER_KEYTAB_KEY);
    String workerPrincipal = conf.get(Constants.WORKER_PRINCIPAL_KEY);

    login(Constants.WORKER_KEYTAB_KEY, workerKeytab, Constants.WORKER_PRINCIPAL_KEY,
        workerPrincipal, host);
  }

  private void login(String keytabFileKey, String keytabFile, String principalKey,
      String principal, String hostname) throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set(keytabFileKey, keytabFile);
    conf.set(principalKey, principal);
    SecurityUtil.login(conf, keytabFileKey, principalKey, hostname);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
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
        Stack<Path> dirsToMake = new Stack<Path>();
        dirsToMake.push(hdfsPath);
        Path parent = hdfsPath.getParent();
        while (!mFileSystem.exists(parent)) {
          dirsToMake.push(parent);
          parent = parent.getParent();
        }
        while (!dirsToMake.empty()) {
          if (!FileSystem.mkdirs(mFileSystem, dirsToMake.pop(), PERMISSION)) {
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
    if (!exists(src)) {
      LOG.error("File {} does not exist. Therefore rename to {} failed.", src, dst);
      return false;
    }

    if (exists(dst)) {
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
  public void setPermission(String path, String posixPerm) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      LOG.info("Changing file '{}' permissions from: {} to {}", fileStatus.getPath(),
          fileStatus.getPermission(), posixPerm);
      FsPermission perm = new FsPermission(Short.parseShort(posixPerm));
      mFileSystem.setPermission(fileStatus.getPath(), perm);
    } catch (IOException e) {
      LOG.error("Fail to set permission for {} with perm {}", path, posixPerm, e);
      throw e;
    }
  }
}
