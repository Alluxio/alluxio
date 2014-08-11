/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.PrefixList;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.CommonConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.UfsUtils;

/**
 * Base class for Apache Hadoop based Tachyon {@link FileSystem}. This class really just delegates
 * to {@link tachyon.client.TachyonFS} for most operations.
 * 
 * All implementing classes must define {@link #isZookeeperMode()} which states if fault tolerant
 * is used and {@link #getScheme()} for Hadoop's {@link java.util.ServiceLoader} support.
 */
abstract class AbstractTFS extends FileSystem {
  public static final String FIRST_COM_PATH = "tachyon_dep/";
  public static final String RECOMPUTE_PATH = "tachyon_recompute/";

  public static String UNDERFS_ADDRESS;

  public static boolean USE_HDFS = true;

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private URI mUri = null;
  private Path mWorkingDir = new Path(Constants.PATH_SEPARATOR);
  private TachyonFS mTFS = null;
  private String mTachyonHeader = null;

  @Override
  public FSDataOutputStream append(Path cPath, int bufferSize, Progressable progress)
      throws IOException {
    LOG.info("append(" + cPath + ", " + bufferSize + ", " + progress + ")");

    String path = Utils.getPathWithoutScheme(cPath);
    fromHdfsToTachyon(path);
    int fileId = mTFS.getFileId(path);
    TachyonFile file = mTFS.getFile(fileId);

    if (file.length() > 0) {
      LOG.warn("This maybe an error.");
    }

    return new FSDataOutputStream(file.getOutStream(WriteType.CACHE_THROUGH), null);
  }

  @Override
  public FSDataOutputStream create(Path cPath, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    LOG.info("create(" + cPath + ", " + permission + ", " + overwrite + ", " + bufferSize + ", "
        + replication + ", " + blockSize + ", " + progress + ")");

    if (!CommonConf.get().ASYNC_ENABLED) {
      String path = Utils.getPathWithoutScheme(cPath);
      if (mTFS.exist(path)) {
        if (!mTFS.delete(path, false)) {
          throw new IOException("Failed to delete existing data " + cPath);
        }
      }
      int fileId = mTFS.createFile(path, blockSize);
      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());
      return new FSDataOutputStream(file.getOutStream(WriteType.CACHE_THROUGH), null);
    }

    if (cPath.toString().contains(FIRST_COM_PATH) && !cPath.toString().contains("SUCCESS")) {
      String path = Utils.getPathWithoutScheme(cPath);
      mTFS.createFile(path, blockSize);
      path = path.substring(path.indexOf(FIRST_COM_PATH) + FIRST_COM_PATH.length());
      path = path.substring(0, path.indexOf(Constants.PATH_SEPARATOR));
      int depId = Integer.parseInt(path);
      LOG.info("create(" + cPath + ") : " + path + " " + depId);
      path = Utils.getPathWithoutScheme(cPath);
      path = path.substring(path.indexOf("part-") + 5);
      int index = Integer.parseInt(path);
      ClientDependencyInfo info = mTFS.getClientDependencyInfo(depId);
      int fileId = info.getChildren().get(index);
      LOG.info("create(" + cPath + ") : " + path + " " + index + " " + info + " " + fileId);

      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());
      // if (file.getBlockSizeByte() != blockSize) {
      // throw new IOException("File already exist with a different blocksize "
      // file.getBlockSizeByte() + " != " + blockSize);
      // }
      return new FSDataOutputStream(file.getOutStream(WriteType.ASYNC_THROUGH), null);
    }
    if (cPath.toString().contains(RECOMPUTE_PATH) && !cPath.toString().contains("SUCCESS")) {
      String path = Utils.getPathWithoutScheme(cPath);
      mTFS.createFile(path, blockSize);
      path = path.substring(path.indexOf(RECOMPUTE_PATH) + RECOMPUTE_PATH.length());
      path = path.substring(0, path.indexOf(Constants.PATH_SEPARATOR));
      int depId = Integer.parseInt(path);
      LOG.info("create(" + cPath + ") : " + path + " " + depId);
      path = Utils.getPathWithoutScheme(cPath);
      path = path.substring(path.indexOf("part-") + 5);
      int index = Integer.parseInt(path);
      ClientDependencyInfo info = mTFS.getClientDependencyInfo(depId);
      int fileId = info.getChildren().get(index);
      LOG.info("create(" + cPath + ") : " + path + " " + index + " " + info + " " + fileId);

      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());
      // if (file.getBlockSizeByte() != blockSize) {
      // throw new IOException("File already exist with a different blocksize "
      // file.getBlockSizeByte() + " != " + blockSize);
      // }
      return new FSDataOutputStream(file.getOutStream(WriteType.ASYNC_THROUGH), null);
    } else {
      String path = Utils.getPathWithoutScheme(cPath);
      int fileId;
      WriteType type = WriteType.CACHE_THROUGH;
      if (mTFS.exist(path)) {
        fileId = mTFS.getFileId(path);
        type = WriteType.MUST_CACHE;
      } else {
        fileId = mTFS.createFile(path, blockSize);
      }

      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());
      // if (file.getBlockSizeByte() != blockSize) {
      // throw new IOException("File already exist with a different blocksize "
      // file.getBlockSizeByte() + " != " + blockSize);
      // }
      return new FSDataOutputStream(file.getOutStream(type), null);
    }
  }

  /**
   * TODO: We need to refactor this method after having a new internal API support (TACHYON-46).
   * <p>
   * Opens an FSDataOutputStream at the indicated Path with write-progress reporting. Same as
   * create(), except fails if parent directory doesn't already exist.
   * 
   * @param cPath
   *          the file name to open
   * @param overwrite
   *          if a file with this name already exists, then if true,
   *          the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize
   *          the size of the buffer to be used.
   * @param replication
   *          required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   * @deprecated API only for 0.20-append
   */
  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path cPath, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    String tPath = Utils.getPathWithoutScheme(cPath.getParent());
    fromHdfsToTachyon(tPath);
    if (!mTFS.exist(tPath)) {
      throw new FileNotFoundException("Parent directory does not exist!");
    }
    return this.create(cPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    LOG.info("delete(" + path + ", " + recursive + ")");
    String tPath = Utils.getPathWithoutScheme(path);
    fromHdfsToTachyon(tPath);
    return mTFS.delete(tPath, recursive);
  }

  private void fromHdfsToTachyon(String path) throws IOException {
    if (!mTFS.exist(path)) {
      Path hdfsPath = Utils.getHDFSPath(path);
      FileSystem fs = hdfsPath.getFileSystem(getConf());
      if (fs.exists(hdfsPath)) {
        String ufsAddrPath = CommonUtils.concat(UNDERFS_ADDRESS, path);
        // Set the path as the TFS root path.
        UfsUtils.loadUnderFs(mTFS, path, ufsAddrPath, new PrefixList(null));
      }
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    if (file == null) {
      return null;
    }

    String path = Utils.getPathWithoutScheme(file.getPath());
    fromHdfsToTachyon(path);
    int fileId = mTFS.getFileId(path);

    if (fileId == -1) {
      throw new FileNotFoundException("File does not exist: " + file.getPath());
    }

    List<BlockLocation> blockLocations = new ArrayList<BlockLocation>();
    List<ClientBlockInfo> blocks = mTFS.getFileBlocks(fileId);
    for (int k = 0; k < blocks.size(); k ++) {
      ClientBlockInfo info = blocks.get(k);
      long offset = info.getOffset();
      long end = offset + info.getLength();
      if ((offset >= start && offset <= start + len) || (end >= start && end <= start + len)) {
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<String> hosts = new ArrayList<String>();
        for (NetAddress addr : info.getLocations()) {
          names.add(addr.mHost);
          hosts.add(addr.mHost);
        }
        blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names), CommonUtils
            .toStringArray(hosts), offset, info.getLength()));
      }
    }

    BlockLocation[] ret = new BlockLocation[blockLocations.size()];
    for (int k = 0; k < blockLocations.size(); k ++) {
      ret[k] = blockLocations.get(k);
    }
    return ret;
  }

  /**
   * Return the status of a single file. If the file does not exist in Tachyon, query it from HDFS.
   */
  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    String tPath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(tPath);

    LOG.info("getFileStatus(" + path + "): HDFS Path: " + hdfsPath + " TPath: " + mTachyonHeader
        + tPath);
    if (USE_HDFS) {
      fromHdfsToTachyon(tPath);
    }
    TachyonFile file = mTFS.getFile(tPath);
    if (file == null) {
      LOG.info("File does not exist: " + path);
      throw new FileNotFoundException("File does not exist: " + path);
    }

    FileStatus ret =
        new FileStatus(file.length(), file.isDirectory(), file.getDiskReplication(),
            file.getBlockSizeByte(), file.getCreationTimeMs(), file.getCreationTimeMs(), null,
            null, null, new Path(mTachyonHeader + tPath));
    return ret;
  }

  /**
   * Returns an object implementing the Tachyon-specific client API.
   * 
   * @return null if initialize() hasn't been called.
   */
  public TachyonFS getTachyonFS() {
    return mTFS;
  }

  @Override
  public URI getUri() {
    return mUri;
  }

  @Override
  public Path getWorkingDirectory() {
    LOG.info("getWorkingDirectory: " + mWorkingDir);
    return mWorkingDir;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    LOG.info("setWorkingDirectory(" + path + ")");
    if (path.isAbsolute()) {
      mWorkingDir = path;
    } else {
      mWorkingDir = new Path(mWorkingDir, path);
    }
  }

  /**
   * Initialize the class, have a lazy connection with Tachyon through mTFS.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    LOG.info("initialize(" + uri + ", " + conf + "). Connecting to Tachyon: " + uri.toString());
    Utils.addS3Credentials(conf);
    setConf(conf);
    mTachyonHeader = getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
    mTFS = TachyonFS.get(uri.getHost(), uri.getPort(), isZookeeperMode());
    mUri = URI.create(mTachyonHeader);
    if (UNDERFS_ADDRESS == null || URI.create(UNDERFS_ADDRESS).getScheme() == null) {
      USE_HDFS = false;
    }
    LOG.info(mTachyonHeader + " " + mUri + " " + UNDERFS_ADDRESS);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    String tPath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(tPath);
    LOG.info("listStatus(" + path + "): HDFS Path: " + hdfsPath);

    fromHdfsToTachyon(tPath);
    if (!mTFS.exist(tPath)) {
      throw new FileNotFoundException("File does not exist: " + path);
    }

    List<ClientFileInfo> files = mTFS.listStatus(tPath);
    FileStatus[] ret = new FileStatus[files.size()];
    for (int k = 0; k < files.size(); k ++) {
      ClientFileInfo info = files.get(k);
      // TODO replicate 3 with the number of disk replications.
      ret[k] =
          new FileStatus(info.getLength(), info.isFolder, 3, info.getBlockSizeByte(),
              info.getCreationTimeMs(), info.getCreationTimeMs(), null, null, null, new Path(
                  mTachyonHeader + info.getPath()));
    }
    return ret;
  }

  @Override
  public boolean mkdirs(Path cPath, FsPermission permission) throws IOException {
    LOG.info("mkdirs(" + cPath + ", " + permission + ")");
    return mTFS.mkdir(Utils.getPathWithoutScheme(cPath));
  }

  @Override
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.info("open(" + cPath + ", " + bufferSize + ")");

    String path = Utils.getPathWithoutScheme(cPath);
    fromHdfsToTachyon(path);
    int fileId = mTFS.getFileId(path);

    return new FSDataInputStream(new HdfsFileInputStream(mTFS, fileId, Utils.getHDFSPath(path),
        getConf(), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename(" + src + ", " + dst + ")");
    String hSrc = Utils.getPathWithoutScheme(src);
    String hDst = Utils.getPathWithoutScheme(dst);
    fromHdfsToTachyon(hSrc);
    return mTFS.rename(hSrc, hDst);
  }

  /**
   * Get the URI schema that maps to the FileSystem. This was introduced in Hadoop 2.x as a means
   * to make loading new FileSystems simpler. This doesn't exist in Hadoop 1.x, so can not put
   * 
   * @Override on this method.
   * 
   * @return schema hadoop should map to.
   * 
   * @see org.apache.hadoop.fs.FileSystem#createFileSystem(java.net.URI,
   *      org.apache.hadoop.conf.Configuration)
   */
  public abstract String getScheme();

  /**
   * Determines if zookeeper should be used for the FileSystem. This method should only be used for
   * {@link #initialize(java.net.URI, org.apache.hadoop.conf.Configuration)}.
   * 
   * @return true if zookeeper should be used
   */
  protected abstract boolean isZookeeperMode();
}
