/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.PrefixList;
import tachyon.TachyonURI;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.ConfUtils;
import tachyon.util.UfsUtils;

/**
 * Base class for Apache Hadoop based Tachyon {@link FileSystem}. This class really just delegates
 * to {@link tachyon.client.TachyonFS} for most operations.
 * 
 * All implementing classes must define {@link #isZookeeperMode()} which states if fault tolerant is
 * used and {@link #getScheme()} for Hadoop's {@link java.util.ServiceLoader} support.
 */
abstract class AbstractTFS extends FileSystem {
  public static final String FIRST_COM_PATH = "tachyon_dep/";
  public static final String RECOMPUTE_PATH = "tachyon_recompute/";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private String mUnderFSAddress;

  private URI mUri = null;
  private Path mWorkingDir = new Path(TachyonURI.SEPARATOR);
  private TachyonFS mTFS = null;
  private String mTachyonHeader = null;
  private final TachyonConf mTachyonConf = new TachyonConf();

  @Override
  public FSDataOutputStream append(Path cPath, int bufferSize, Progressable progress)
      throws IOException {
    LOG.info("append(" + cPath + ", " + bufferSize + ", " + progress + ")");
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    fromHdfsToTachyon(path);
    int fileId = mTFS.getFileId(path);
    TachyonFile file = mTFS.getFile(fileId);

    if (file.length() > 0) {
      LOG.warn("This maybe an error.");
    }

    WriteType type = getWriteType();
    return new FSDataOutputStream(file.getOutStream(type), null);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (mTFS != null) {
        mTFS.close();
      }
    }
  }

  @Override
  public FSDataOutputStream create(Path cPath, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    LOG.info("create(" + cPath + ", " + permission + ", " + overwrite + ", " + bufferSize + ", "
        + replication + ", " + blockSize + ", " + progress + ")");

    boolean asyncEnabled = mTachyonConf.getBoolean(Constants.ASYNC_ENABLED, true);
    if (!asyncEnabled) {
      TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
      if (mTFS.exist(path)) {
        if (!mTFS.delete(path, false)) {
          throw new IOException("Failed to delete existing data " + cPath);
        }
      }
      int fileId = mTFS.createFile(path, blockSize);
      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());

      WriteType type = getWriteType();
      return new FSDataOutputStream(file.getOutStream(type), null);
    }

    if (cPath.toString().contains(FIRST_COM_PATH) && !cPath.toString().contains("SUCCESS")) {
      TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
      mTFS.createFile(path, blockSize);
      String depPath = path.getPath();
      depPath = depPath.substring(depPath.indexOf(FIRST_COM_PATH) + FIRST_COM_PATH.length());
      depPath = depPath.substring(0, depPath.indexOf(TachyonURI.SEPARATOR));
      int depId = Integer.parseInt(depPath);
      LOG.info("create(" + cPath + ") : " + depPath + " " + depId);
      depPath = path.getPath();
      depPath = depPath.substring(depPath.indexOf("part-") + 5);
      int index = Integer.parseInt(depPath);
      ClientDependencyInfo info = mTFS.getClientDependencyInfo(depId);
      int fileId = info.getChildren().get(index);
      LOG.info("create(" + cPath + ") : " + depPath + " " + index + " " + info + " " + fileId);

      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());
      // if (file.getBlockSizeByte() != blockSize) {
      // throw new IOException("File already exist with a different blocksize "
      // file.getBlockSizeByte() + " != " + blockSize);
      // }
      return new FSDataOutputStream(file.getOutStream(WriteType.ASYNC_THROUGH), null);
    }

    if (cPath.toString().contains(RECOMPUTE_PATH) && !cPath.toString().contains("SUCCESS")) {
      TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
      mTFS.createFile(path, blockSize);
      String depPath = path.getPath();
      depPath = depPath.substring(depPath.indexOf(RECOMPUTE_PATH) + RECOMPUTE_PATH.length());
      depPath = depPath.substring(0, depPath.indexOf(TachyonURI.SEPARATOR));
      int depId = Integer.parseInt(depPath);
      LOG.info("create(" + cPath + ") : " + depPath + " " + depId);
      depPath = path.getPath();
      depPath = depPath.substring(depPath.indexOf("part-") + 5);
      int index = Integer.parseInt(depPath);
      ClientDependencyInfo info = mTFS.getClientDependencyInfo(depId);
      int fileId = info.getChildren().get(index);
      LOG.info("create(" + cPath + ") : " + depPath + " " + index + " " + info + " " + fileId);

      TachyonFile file = mTFS.getFile(fileId);
      file.setUFSConf(getConf());
      // if (file.getBlockSizeByte() != blockSize) {
      // throw new IOException("File already exist with a different blocksize "
      // file.getBlockSizeByte() + " != " + blockSize);
      // }
      return new FSDataOutputStream(file.getOutStream(WriteType.ASYNC_THROUGH), null);
    } else {
      TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
      int fileId;
      WriteType type =  getWriteType();
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
   * @param cPath the file name to open
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *        overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
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
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath.getParent()));
    fromHdfsToTachyon(path);
    if (!mTFS.exist(path)) {
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
  public boolean delete(Path cPath, boolean recursive) throws IOException {
    LOG.info("delete(" + cPath + ", " + recursive + ")");
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    fromHdfsToTachyon(path);
    return mTFS.delete(path, recursive);
  }

  private void fromHdfsToTachyon(TachyonURI path) throws IOException {
    if (!mTFS.exist(path)) {
      Path hdfsPath = Utils.getHDFSPath(path, mUnderFSAddress);
      Configuration conf = new Configuration(getConf());
      if (conf.get("fs.defaultFS") == null) {
        conf.set("fs.defaultFS", mUnderFSAddress);
      }
      FileSystem fs = hdfsPath.getFileSystem(conf);
      if (fs.exists(hdfsPath)) {
        TachyonURI ufsUri = new TachyonURI(mUnderFSAddress);
        TachyonURI ufsAddrPath = new TachyonURI(ufsUri.getScheme(), ufsUri.getAuthority(),
            path.getPath());
        // Set the path as the TFS root path.
        UfsUtils.loadUnderFs(mTFS, path, ufsAddrPath, new PrefixList(null), mTachyonConf);
      }
    }
  }

  @Override
  public long getDefaultBlockSize() {
    return mTachyonConf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE,
        Constants.DEFAULT_BLOCK_SIZE_BYTE);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    if (file == null) {
      return null;
    }

    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(file.getPath()));
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
    TachyonURI tPath = new TachyonURI(Utils.getPathWithoutScheme(path));
    Path hdfsPath = Utils.getHDFSPath(tPath, mUnderFSAddress);

    LOG.info("getFileStatus(" + path + "): HDFS Path: " + hdfsPath + " TPath: " + mTachyonHeader
        + tPath);
    if (useHdfs()) {
      fromHdfsToTachyon(tPath);
    }
    TachyonFile file = mTFS.getFile(tPath);
    if (file == null) {
      LOG.info("File does not exist: " + path);
      throw new FileNotFoundException("File does not exist: " + path);
    }

    FileStatus ret =
        new FileStatus(file.length(), file.isDirectory(), file.getDiskReplication(),
            file.getBlockSizeByte(), file.getCreationTimeMs(), file.getCreationTimeMs(),
            new FsPermission(file.getPermission()), file.getOwner(), file.getGroup(), new Path(
                mTachyonHeader + tPath));
    return ret;
  }

  /**
   * Set owner of a path (i.e. a file or a directory). The parameters username and groupname cannot
   * both be null.
   * 
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(Path p, final String username, final String groupname) throws IOException {
    LOG.info("chown owner:" + username + " group:" + groupname + " on " + p);
    TachyonURI tPath = new TachyonURI(Utils.getPathWithoutScheme(p));
    fromHdfsToTachyon(tPath);
    mTFS.setOwner(tPath, username, groupname, false);
  }

  /**
   * Set permission of a path.
   * 
   * @param p The path
   * @param permission
   */
  public void setPermission(Path p, FsPermission permission) throws IOException {
    LOG.info("chmod perm:" + permission.toString() + " on " + p);
    TachyonURI tPath = new TachyonURI(Utils.getPathWithoutScheme(p));
    fromHdfsToTachyon(tPath);
    mTFS.setPermission(tPath, permission.toShort(), false);
  }

  /**
   * Get the URI schema that maps to the FileSystem. This was introduced in Hadoop 2.x as a means to
   * make loading new FileSystems simpler. This doesn't exist in Hadoop 1.x, so can not put
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

    // Load TachyonConf if any and merge to the one in TachyonFS
    TachyonConf siteConf = ConfUtils.loadFromHadoopConfiguration(conf);
    if (siteConf != null) {
      mTachyonConf.merge(siteConf);
    }
    mTachyonConf.set(Constants.MASTER_HOSTNAME, uri.getHost());
    mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(uri.getPort()));
    mTachyonConf.set(Constants.USE_ZOOKEEPER, Boolean.toString(isZookeeperMode()));

    mTFS = TachyonFS.get(mTachyonConf);

    mUri = URI.create(mTachyonHeader);
    mUnderFSAddress = mTFS.getUfsAddress();
    LOG.info(mTachyonHeader + " " + mUri + " " + mUnderFSAddress);
  }

  /**
   * Determines if zookeeper should be used for the FileSystem. This method should only be used for
   * {@link #initialize(java.net.URI, org.apache.hadoop.conf.Configuration)}.
   * 
   * @return true if zookeeper should be used
   */
  protected abstract boolean isZookeeperMode();

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    TachyonURI tPath = new TachyonURI(Utils.getPathWithoutScheme(path));
    Path hdfsPath = Utils.getHDFSPath(tPath, mUnderFSAddress);
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
              info.getCreationTimeMs(), info.getCreationTimeMs(),
              new FsPermission((short)info.getPermission()), info.getOwner(),
              info.getGroup(), new Path(mTachyonHeader + info.getPath()));
    }
    return ret;
  }

  @Override
  public boolean mkdirs(Path cPath, FsPermission permission) throws IOException {
    LOG.info("mkdirs(" + cPath + ", " + permission + ")");
    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    return mTFS.mkdir(path);
  }

  @Override
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.info("open(" + cPath + ", " + bufferSize + ")");

    TachyonURI path = new TachyonURI(Utils.getPathWithoutScheme(cPath));
    fromHdfsToTachyon(path);
    int fileId = mTFS.getFileId(path);

    return new FSDataInputStream(new HdfsFileInputStream(mTFS, fileId, Utils.getHDFSPath(path,
        mUnderFSAddress), getConf(), bufferSize, mTachyonConf));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename(" + src + ", " + dst + ")");
    TachyonURI srcPath = new TachyonURI(Utils.getPathWithoutScheme(src));
    TachyonURI dstPath = new TachyonURI(Utils.getPathWithoutScheme(dst));
    fromHdfsToTachyon(srcPath);
    return mTFS.rename(srcPath, dstPath);
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
   * When underfs has a schema, then we can use the hdfs underfs code base.
   * <p />
   * When this check is not done, {@link #fromHdfsToTachyon(TachyonURI)} is called, which loads the
   * default filesystem (hadoop's). When there is no schema, then it may default to tachyon which
   * causes a recursive loop.
   *
   * @see <a href="https://tachyon.atlassian.net/browse/TACHYON-54">TACHYON-54</a>
   */
  @Deprecated
  private boolean useHdfs() {
    return mUnderFSAddress != null && URI.create(mUnderFSAddress).getScheme() != null;
  }

  private WriteType getWriteType() {
    return mTachyonConf.getEnum(Constants.USER_DEFAULT_WRITE_TYPE, WriteType.CACHE_THROUGH);
  }
}
