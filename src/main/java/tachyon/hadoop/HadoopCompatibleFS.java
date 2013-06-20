package tachyon.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;
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

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.thrift.NetAddress;

/**
 * An Hadoop FileSystem interface implementation. Any program working with Hadoop HDFS can work
 * with Tachyon transparently by using this class. However, it is not as efficient as using
 * the Tachyon API in tachyon.client package.
 */
public class HadoopCompatibleFS extends FileSystem {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private URI mUri = null;
  private Path mWorkingDir = new Path("/");
  private TachyonFS mTFS = null;
  private String mTachyonHeader = null;

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    LOG.info("TachyonFileSystem append(" + path + ", " + bufferSize + ", " + progress + ")");
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path cPath, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
          throws IOException {
    LOG.debug("TachyonFileSystem create(" + cPath + ", " + permission + ", " + overwrite + 
        ", " + bufferSize + ", " + replication + ", " + blockSize + ", " + progress + ")");

    String path = Utils.getPathWithoutScheme(cPath);

    Path hdfsPath = Utils.getHDFSPath(path);
    FileSystem fs = hdfsPath.getFileSystem(getConf());
    LOG.debug("TachyonFileSystem mkdirs: making dir " + hdfsPath);

    return fs.create(hdfsPath, permission, overwrite, bufferSize, replication, blockSize,
        progress);
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    LOG.debug("TachyonFileSystem delete(" + path + ", " + recursive + ")");
    Path hdfsPath = Utils.getHDFSPath(path);
    FileSystem fs = hdfsPath.getFileSystem(getConf());
    LOG.debug("TachyonFileSystem delete(" + hdfsPath + ", " + recursive + ")");
    boolean succeed = false;
    succeed = mTFS.delete(Utils.getPathWithoutScheme(path), recursive);
    return fs.delete(hdfsPath, recursive) && succeed;
  }

  @Override
  /**
   * Return the status of a single file.
   * 
   * If the file does not exist in Tachyon, query it from HDFS. 
   */
  public FileStatus getFileStatus(Path path) throws IOException {
    String filePath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(filePath);

    LOG.debug("TachyonFileSystem getFilesStatus(" + path + "): Corresponding HDFS Path: " + hdfsPath);

    FileSystem fs = hdfsPath.getFileSystem(getConf());
    FileStatus hfs = fs.getFileStatus(hdfsPath);

    if (!hfs.isDir()) {
      int fileId;
      fileId = mTFS.getFileId(filePath);
      if (fileId > 0) {
        LOG.debug("Tachyon has file " + filePath);
      } else {
        LOG.debug("Tachyon does not have file " + filePath);
        int tmp = mTFS.createFile(filePath, hdfsPath.toString());
        if (tmp == -1) {
          LOG.debug("Tachyon does have file " + filePath + " and creation failed.");
        } else {
          LOG.debug("Tachyon does not have file " + filePath + " checkpoint added.");
        }
      }
    }

    FileStatus ret = new FileStatus(hfs.getLen(), hfs.isDir(), hfs.getReplication(),
        Integer.MAX_VALUE, hfs.getModificationTime(), hfs.getAccessTime(), hfs.getPermission(),
        hfs.getOwner(), hfs.getGroup(), new Path(mTachyonHeader + filePath));
    LOG.debug(mTachyonHeader + filePath);

    LOG.debug("HFS: " + Utils.toStringHadoopFileStatus(hfs));
    LOG.debug("TFS: " + Utils.toStringHadoopFileStatus(ret));

    return ret;
  }

  @Override
  public URI getUri() {
    return mUri;
  }

  @Override
  public Path getWorkingDirectory() {
    LOG.debug("TachyonFileSystem getWorkingDirectory() with return " + mWorkingDir);
    return mWorkingDir;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) 
      throws IOException {
    if (file == null) {
      return null;
    }
    String path = Utils.getPathWithoutScheme(file.getPath());
    BlockLocation ret = null;

    int fileId = -1;
    ArrayList<String> names = new ArrayList<String>();
    ArrayList<String> hosts = new ArrayList<String>();

    fileId = mTFS.getFileId(path);

    if (fileId != -1) {
      List<NetAddress> locations = mTFS.getClientBlockInfo(fileId, 0).getLocations();
      if (locations != null) {
        for (int k = 0; k < locations.size(); k ++) {
          names.add(locations.get(k).mHost);
          hosts.add(locations.get(k).mHost);
        }

        if (hosts.size() > 0) {
          ret = new BlockLocation(CommonUtils.toStringArray(names),
              CommonUtils.toStringArray(hosts), 0, Long.MAX_VALUE);
        }
      }
    }

    if (ret == null) {
      Path hdfsPath = Utils.getHDFSPath(path);
      FileSystem fs = hdfsPath.getFileSystem(getConf());
      FileStatus hdfsFileStatus = new FileStatus(file.getLen(), file.isDir(),
          file.getReplication(), file.getBlockSize(), file.getModificationTime(), 
          file.getAccessTime(), file.getPermission(), file.getOwner(), file.getGroup(),
          hdfsPath);
      return fs.getFileBlockLocations(hdfsFileStatus, 0, 1);
    }

    BlockLocation[] res = new BlockLocation[1];
    res[0] = ret;
    return res;
  }

  @Override
  /**
   * Initialize the class, have a lazy connection with Tachyon through mTC.
   */
  public void initialize(URI uri, Configuration conf) throws IOException {
    LOG.debug("TachyonFileSystem initialize(" + uri + ", " + conf + "). Connecting TachyonSystem: " +
        uri.getHost() + ":" + uri.getPort());
    mTFS = TachyonFS.get(new InetSocketAddress(uri.getHost(), uri.getPort()));
    mTachyonHeader = "tachyon://" + uri.getHost() + ":" + uri.getPort() + "";
    Utils.HDFS_ADDRESS = mTFS.getUnderfsAddress();
    mUri = URI.create("tachyon://" + uri.getHost() + ":" + uri.getPort());
  }

  @Override
  /**
   * Return all files in the path.
   */
  public FileStatus[] listStatus(Path path) throws IOException {
    String filePath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(filePath);
    LOG.debug("TachyonFileSystem listStatus(" + path + "): Corresponding HDFS Path: " + hdfsPath);
    FileSystem fs = hdfsPath.getFileSystem(getConf());
    FileStatus[] hfs = fs.listStatus(hdfsPath);
    ArrayList<FileStatus> tRet = new ArrayList<FileStatus>();
    for (int k = 0; k < hfs.length; k ++) {
      if (hfs[k].isDir()) {
        FileStatus[] tFileStatus = listStatus(hfs[k].getPath());
        for (FileStatus tfs : tFileStatus) {
          tRet.add(tfs);
        }
      } else {
        tRet.add(getFileStatus(hfs[k].getPath()));
      }
    }
    FileStatus[] ret = new FileStatus[hfs.length];
    ret = tRet.toArray(ret);

    return ret;
  }

  @Override
  public boolean mkdirs(Path cPath, FsPermission permission) throws IOException {
    LOG.debug("TachyonFileSystem mkdirs(" + cPath + ", " + permission + ")");

    String path = Utils.getPathWithoutScheme(cPath);
    Path hdfsPath = Utils.getHDFSPath(path);
    FileSystem fs = hdfsPath.getFileSystem(getConf());
    LOG.debug("TachyonFileSystem mkdirs: making dir " + hdfsPath);
    return fs.mkdirs(hdfsPath);
  }

  @Override
  /**
   * Return the inputstream of a file.
   */
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.debug("TachyonFileSystem open(" + cPath + ", " + bufferSize + ")");

    String path = Utils.getPathWithoutScheme(cPath);

    String rawPath = path;
    int fileId = -1;

    fileId = mTFS.getFileId(path);

    Path hdfsPath = Utils.getHDFSPath(rawPath);
    if (fileId == -1) {
      FileSystem fs = hdfsPath.getFileSystem(getConf());
      return fs.open(hdfsPath, bufferSize);
    }

    return new FSDataInputStream(new TFileInputStreamHdfs(mTFS, fileId,
        hdfsPath, getConf(), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("TachyonFileSystem rename(" + src + ", " + dst + ")");
    Path hSrc = Utils.getHDFSPath(src);
    Path hDst = Utils.getHDFSPath(dst);
    FileSystem fs = hSrc.getFileSystem(getConf());
    boolean succeed = false;
    succeed = mTFS.rename(Utils.getPathWithoutScheme(src), Utils.getPathWithoutScheme(dst));
    return fs.rename(hSrc, hDst) && succeed;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    LOG.debug("TachyonFileSystem setWorkingDirectory(" + path + ")");
    if (path.isAbsolute()) {
      mWorkingDir = path;
    } else {
      mWorkingDir = new Path(mWorkingDir, path);
    }
  }
}