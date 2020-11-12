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

package alluxio.underfs.cosn;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Tencent Cloud COSN {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class CosnUnderFileSystem extends ConsistentUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(CosnUnderFileSystem.class);
  private FileSystem mFileSystem;

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
    Configuration cosnConf = new Configuration();
    // Configure for Cosn FS

    Preconditions.checkArgument(conf.isSet(PropertyKey.COSN_REGION),
        "Property %s is required to connect to COS", PropertyKey.COSN_REGION);
    Preconditions.checkArgument(conf.isSet(PropertyKey.COSN_ACCESS_KEY),
        "Property %s is required to connect to COS", PropertyKey.COSN_ACCESS_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.COSN_SECRET_KEY),
        "Property %s is required to connect to COS", PropertyKey.COSN_SECRET_KEY);

    cosnConf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
    cosnConf.set("fs.AbstractFileSystem.cosn.impl", "org.apache.hadoop.fs.CosN");

    cosnConf.set("fs.cosn.bucket.region", conf.get(PropertyKey.COSN_REGION));
    cosnConf.set("fs.cosn.userinfo.secretId", conf.get(PropertyKey.COSN_ACCESS_KEY));
    cosnConf.set("fs.cosn.userinfo.secretKey", conf.get(PropertyKey.COSN_SECRET_KEY));

    if (conf.isSet(PropertyKey.COSN_BLOCK_SIZE_KEY)) {
      cosnConf.set("fs.cosn.block.size", conf.get(PropertyKey.COSN_BLOCK_SIZE_KEY));
    }

    if (conf.isSet(PropertyKey.COSN_UPLOAD_THREAD_POOL_SIZE_KEY)) {
      cosnConf.set("fs.cosn.upload_thread_pool",
              conf.get(PropertyKey.COSN_UPLOAD_THREAD_POOL_SIZE_KEY));
    }

    if (conf.isSet(PropertyKey.COSN_READ_AHEAD_QUEUE_SIZE)) {
      cosnConf.set("fs.cosn.read.ahead.queue.size",
              conf.get(PropertyKey.COSN_READ_AHEAD_QUEUE_SIZE));
    }
    return cosnConf;
  }

  /**
   * Constructs a new instance of {@link Cosn UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link CosnUnderFileSystem} instance
   */
  public static CosnUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    LOG.info("Initializing COSN underFs");
    Configuration cosnConf = createConfiguration(conf);
    FileSystem cosnfs = new CosFileSystem();
    try {
      URI cosnUri = new URI(uri.toString());
      cosnfs.initialize(cosnUri, cosnConf);
    } catch (URISyntaxException ue) {
      throw new RuntimeException("Failed to create cosn FileSystem", ue);
    } catch (IOException ie) {
      throw new RuntimeException("Failed to create cosn FileSystem", ie);
    }
    return new CosnUnderFileSystem(uri, cosnfs, conf);
  }

  /**
   * Constructor for {@link CosnUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param cosnConf the configuration for this Cosn ufs
   */
  protected CosnUnderFileSystem(AlluxioURI uri,
                    FileSystem cosnfs,
                    UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mFileSystem = cosnfs;
  }

  @Override
  public String getUnderFSType() {
    return "cosn";
  }

  @Override
  public void close() throws IOException {
    // Don't close; file systems are singletons and closing it here could break other users
    return;
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    try {
      Progressable process = new Progressable() {
        @Override
        public void progress() {
        }
      };
      return mFileSystem.create(new Path(path),
                        new FsPermission(options.getMode().toShort()),
                        false, (int) 0, (short) 0, (long) 0, process);
    } catch (IOException e) {
      LOG.warn("create path {} fail, exception:{} ", path, e.getMessage());
      throw e;
    }
  }

  @Override
  public OutputStream createNonexistingFile(String path) throws IOException {
    Progressable process = new Progressable() {
      @Override
      public void progress() {
      }
    };
    return mFileSystem.create(new Path(path), new FsPermission((short) 0),
            false, (int) 0, (short) 0, (long) 0, process);
  }

  @Override
  public OutputStream createNonexistingFile(String path, CreateOptions options) throws IOException {
    return create(path, options);
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
  public boolean deleteExistingFile(String path) throws IOException {
    return isFile(path) && delete(path, false);
  }

  @Override
  public boolean deleteExistingDirectory(String path, DeleteOptions options) throws IOException {
    return deleteDirectory(path, options);
  }

  @Override
  public boolean deleteExistingDirectory(String path) throws IOException {
    return isDirectory(path) && deleteDirectory(path);
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
    int retry = 3;
    while (retry > 0) {
      try {
        return mFileSystem.delete(new Path(path), recursive);
      } catch (IOException e) {
        LOG.warn("Retry count {} : {}", retry, e.getMessage());
        te = e;
        --retry;
      }
    }
    throw te;
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mFileSystem.exists(new Path(path));
  }

  @Override
  public boolean isExistingDirectory(String path) throws IOException {
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
  public UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException {
    return getDirectoryStatus(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    String contentHash =
        UnderFileSystemUtils.approximateContentHash(fs.getLen(), fs.getModificationTime());
    return new UfsFileStatus(path, contentHash, fs.getLen(), fs.getModificationTime(),
        fs.getOwner(), fs.getGroup(), fs.getPermission().toShort());
  }

  @Override
  public UfsFileStatus getExistingFileStatus(String path) throws IOException {
    return getFileStatus(path);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    Path tPath = new Path(path);
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    if (fs.isFile()) {
      // Return file status.
      String contentHash =
          UnderFileSystemUtils.approximateContentHash(fs.getLen(), fs.getModificationTime());
      UfsStatus ret =  new UfsFileStatus(path, contentHash, fs.getLen(), fs.getModificationTime(),
              fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(),
              mFileSystem.getDefaultBlockSize());
      return ret;
    }

    // Return directory status.
    UfsStatus ret =  new UfsDirectoryStatus(path, fs.getOwner(), fs.getGroup(),
              fs.getPermission().toShort());
    return ret;
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    return getStatus(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      if (fileStatus.isDirectory()) {
        return true;
      }
      return false;
    } catch (IOException ignored) {
      return false;
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      if (fileStatus.isFile()) {
        return true;
      }
      return false;
    } catch (IOException ignored) {
      return false;
    }
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
      if (status.isFile()) {
        String contentHash =
            UnderFileSystemUtils.approximateContentHash(status.getLen(),
            status.getModificationTime());
        retStatus = new UfsFileStatus(status.getPath().getName(), contentHash, status.getLen(),
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
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    try {
      return mFileSystem.mkdirs(new Path(path), new FsPermission(options.getMode().toShort()));
    } catch (IOException e) {
      throw e;
    }
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    IOException te = null;
    int retry = 3;
    while (retry > 0) {
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
        LOG.warn("{} try to open {} : {}", retry, path, e.getMessage());
        te = e;
        retry--;
      }
    }
    throw te;
  }

  @Override
  public InputStream openExistingFile(String path, OpenOptions options) throws IOException {
    IOException te = null;
    int retry = 3;
    while (retry > 0) {
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
        LOG.warn("{} try to open {} : {}", retry, path, e.getMessage());
        te = e;
        retry--;
      }
    }
    throw te;
  }

  @Override
  public InputStream openExistingFile(String path) throws IOException {
    IOException te = null;
    int retry = 3;
    while (retry > 0) {
      try {
        FSDataInputStream inputStream = mFileSystem.open(new Path(path));
        return inputStream;
      } catch (IOException e) {
        LOG.warn("{} try to open {} : {}", retry, path, e.getMessage());
        te = e;
        retry--;
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
  public void setMode(String path, short mode) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
    } catch (IOException e) {
      LOG.warn("Fail to set permission for {} with perm {} : {}", path, mode, e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    return;
  }

  @Override
  public boolean renameRenamableFile(String src, String dst) throws IOException {
    return renameFile(src, dst);
  }

  @Override
  public boolean renameRenamableDirectory(String src, String dst) throws IOException {
    return renameDirectory(src, dst);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return 0;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    List<String> ret = new ArrayList<>();
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path,
            FileLocationOptions options) throws IOException {
    List<String> ret = new ArrayList<>();
    return ret;
  }

  @Override
  public void connectFromWorker(String host) throws IOException {
    return;
  }

  @Override
  public void connectFromMaster(String host) throws IOException {
    return;
  }

  @Override
  public void cleanup() throws IOException {
    return;
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
    } catch (IOException e) {
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
    int retry = 3;
    while (retry > 0) {
      try {
        return mFileSystem.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        LOG.warn("{} try to rename {} to {} : {}", retry, src, dst,
            e.getMessage());
        te = e;
        retry--;
      }
    }
    throw te;
  }
}
