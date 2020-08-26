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
import alluxio.Constants;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.grpc.OpenFilePOptions;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;

import com.google.common.base.Preconditions;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * An Alluxio client compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface, using Alluxio local cache. This client will first consult the local cache before
 * requesting the remote Hadoop FileSystem in case of cache misses.
 */
public class LocalCacheFileSystem extends org.apache.hadoop.fs.FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheFileSystem.class);
  private static final Set<String> SUPPORTED_FS = new HashSet<String>() {
    {
      add(Constants.SCHEME);
      add("ws");
    }
  };

  /** The external Hadoop filesystem to query on cache miss. */
  private final org.apache.hadoop.fs.FileSystem mExternalFileSystem;
  /** Wrapper as an Alluxio filesystem of the external Hadoop filesystem. */
  private AlluxioHdfsFileSystem mExternalFileSystemWrapper;
  private CacheManager mCacheManager;
  private org.apache.hadoop.conf.Configuration mHadoopConf;

  /**
   * @param fileSystem File System instance
   */
  public LocalCacheFileSystem(org.apache.hadoop.fs.FileSystem fileSystem) {
    mExternalFileSystem = Preconditions.checkNotNull(fileSystem, "filesystem");
  }

  @Override
  public synchronized void initialize(URI uri, org.apache.hadoop.conf.Configuration conf)
      throws IOException {
    if (!SUPPORTED_FS.contains(uri.getScheme())) {
      throw new UnsupportedOperationException(
          uri.getScheme() + " is not supported as the external filesystem.");
    }
    super.initialize(uri, conf);
    mHadoopConf = conf;

    // Set statistics
    setConf(conf);

    // Handle metrics
    Properties metricsProperties = new Properties();
    for (Map.Entry<String, String> entry : conf) {
      metricsProperties.setProperty(entry.getKey(), entry.getValue());
    }
    MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProperties));
    mExternalFileSystemWrapper = new AlluxioHdfsFileSystem(mExternalFileSystem, conf);
    mCacheManager = CacheManager.Factory.get(mExternalFileSystemWrapper.getConf());
  }

  @Override
  public void close() throws IOException {
    // super.close should be called first before releasing the resources in this instance, as the
    // super class may invoke other methods in this class. For example,
    // org.apache.hadoop.fs.FileSystem.close may check the existence of certain temp files before
    // closing
    super.close();
    mExternalFileSystem.close();
  }

  /**
   * @return scheme
   */
  // @Override This doesn't exist in Hadoop 1.x, so cannot put {@literal @Override}.
  public String getScheme() {
    return mExternalFileSystem.getScheme();
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    if (mCacheManager == null) {
      return mExternalFileSystem.open(path, bufferSize);
    }
    return new FSDataInputStream(new HdfsFileInputStream(
        new LocalCacheFileInStream(new AlluxioURI(path.toString()),
            OpenFilePOptions.getDefaultInstance(), mExternalFileSystemWrapper, mCacheManager),
        statistics));
  }

  /**
   * Attempts to open the specified file for reading.
   *
   * @param status the status of the file to open
   * @param bufferSize stream buffer size in bytes, currently unused
   * @return an {@link FSDataInputStream} at the indicated path of a file
   */
  public FSDataInputStream open(URIStatus status, int bufferSize) throws IOException {
    if (mCacheManager == null) {
      return mExternalFileSystem.open(HadoopUtils.toPath(new AlluxioURI(status.getPath())),
          bufferSize);
    }
    return new FSDataInputStream(new HdfsFileInputStream(new LocalCacheFileInStream(status,
        OpenFilePOptions.getDefaultInstance(), mExternalFileSystemWrapper, mCacheManager),
        statistics));
  }

  @Override
  public URI getUri() {
    return mExternalFileSystem.getUri();
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return mExternalFileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize,
        progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return mExternalFileSystem.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return mExternalFileSystem.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return mExternalFileSystem.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return mExternalFileSystem.listStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    mExternalFileSystem.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return mExternalFileSystem.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mExternalFileSystem.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return mExternalFileSystem.getFileStatus(f);
  }
}
