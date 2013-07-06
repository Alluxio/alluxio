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
import tachyon.client.TachyonFile;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;

/**
 * An Hadoop FileSystem interface implementation. Any program working with Hadoop HDFS can work
 * with Tachyon transparently by using this class. However, it is not as efficient as using
 * the Tachyon API in tachyon.client package.
 */
public class TFS extends FileSystem {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private URI mUri = null;
  private Path mWorkingDir = new Path("/");
  private TachyonFS mTFS = null;
  private String mTachyonHeader = null;

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    LOG.info("append(" + path + ", " + bufferSize + ", " + progress + ")");
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path cPath, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
          throws IOException {
    LOG.info("create(" + cPath + ", " + permission + ", " + overwrite + 
        ", " + bufferSize + ", " + replication + ", " + blockSize + ", " + progress + ")");

    String path = Utils.getPathWithoutScheme(cPath);
    Path hdfsPath = Utils.getHDFSPath(path);
    FileSystem fs = hdfsPath.getFileSystem(getConf());
    return fs.create(hdfsPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    LOG.debug("delete(" + path + ", " + recursive + ")");
    String tPath = Utils.getPathWithoutScheme(path);
    if (!mTFS.exist(tPath)) {
      getFileStatus(path);
    }
    return mTFS.delete(tPath, recursive);
  }

  @Override
  /**
   * Return the status of a single file.
   * 
   * If the file does not exist in Tachyon, query it from HDFS. 
   */
  public FileStatus getFileStatus(Path path) throws IOException {
    String tPath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(tPath);

    LOG.debug("getFileStatus(" + path + "): HDFS Path: " + hdfsPath + 
        " TPath: " + mTachyonHeader + tPath);

    TachyonFile file = mTFS.getFile(tPath);
    FileStatus ret;
    if (file != null) {
      ret = new FileStatus(file.length(), file.isDirectory(), file.getDiskReplication(),
          file.getBlockSizeByte(), file.getCreationTimeMs(), file.getCreationTimeMs(),
          null, null, null, new Path(mTachyonHeader + tPath));
    } else {
      FileSystem fs = hdfsPath.getFileSystem(getConf());
      FileStatus hfs = fs.getFileStatus(hdfsPath);

      int fid;
      String type;
      if (hfs.isDir()) {
        fid = mTFS.mkdir(tPath);
        type = " directory ";
      } else {
        fid = mTFS.createFile(tPath, hdfsPath.toString());
        type = " file ";
      }

      if (fid == -1) {
        LOG.debug("Tachyon does have" + type + tPath + " and creation failed.");
      } else {
        LOG.debug("Tachyon does not have" + type + tPath + " and creation succeed.");
      }

      ret = new FileStatus(hfs.getLen(), hfs.isDir(), hfs.getReplication(),
          Integer.MAX_VALUE, hfs.getModificationTime(), hfs.getAccessTime(), hfs.getPermission(),
          hfs.getOwner(), hfs.getGroup(), new Path(mTachyonHeader + tPath));

      LOG.debug("HFS: " + Utils.toStringHadoopFileStatus(hfs));
      LOG.debug("TFS: " + Utils.toStringHadoopFileStatus(ret));
    }

    return ret;
  }

  @Override
  public URI getUri() {
    return mUri;
  }

  @Override
  public Path getWorkingDirectory() {
    LOG.debug("getWorkingDirectory: " + mWorkingDir);
    return mWorkingDir;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) 
      throws IOException {
    if (file == null) {
      return null;
    }

    String path = Utils.getPathWithoutScheme(file.getPath());
    List<BlockLocation> blockLocations = new ArrayList<BlockLocation>();

    int fileId = -1;

    fileId = mTFS.getFileId(path);

    if (fileId == -1) {
      getFileStatus(file.getPath());
      fileId = mTFS.getFileId(path);
    }

    if (fileId != -1) {
      List<ClientBlockInfo> blocks = mTFS.getFileBlocks(fileId);
      for (int k = 0; k < blocks.size(); k ++) {
        ClientBlockInfo info = blocks.get(k);
        long offset = info.getOffset();
        long end = offset + info.getLength();
        if ((offset >= start && offset <= start + len) ||
            (end >= start && end <= start + len)) {
          ArrayList<String> names = new ArrayList<String>();
          ArrayList<String> hosts = new ArrayList<String>();
          for (NetAddress addr : info.getLocations()) {
            names.add(addr.mHost);
            hosts.add(addr.mHost);
          }
          blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names),
              CommonUtils.toStringArray(hosts), offset, info.getLength()));
        }
      }
    }

    //    if (blockLocations == null) {
    //      Path hdfsPath = Utils.getHDFSPath(path);
    //      FileSystem fs = hdfsPath.getFileSystem(getConf());
    //      FileStatus hdfsFileStatus = new FileStatus(file.getLen(), file.isDir(),
    //          file.getReplication(), file.getBlockSize(), file.getModificationTime(), 
    //          file.getAccessTime(), file.getPermission(), file.getOwner(), file.getGroup(),
    //          hdfsPath);
    //      return fs.getFileBlockLocations(hdfsFileStatus, 0, 1);
    //    }

    BlockLocation[] ret = new BlockLocation[blockLocations.size()];
    for (int k = 0; k < blockLocations.size(); k ++) {
      ret[k] = blockLocations.get(k);
    }
    return ret;
  }

  /**
   * Initialize the class, have a lazy connection with Tachyon through mTC.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    LOG.debug("initialize(" + uri + ", " + conf + "). Connecting to Tachyon: " + uri.toString());
    mTFS = TachyonFS.get(new InetSocketAddress(uri.getHost(), uri.getPort()));
    mTachyonHeader = "tachyon://" + uri.getHost() + ":" + uri.getPort();
    mUri = URI.create(mTachyonHeader);
    Utils.HDFS_ADDRESS = mTFS.getUnderfsAddress();
  }

  @Override
  /**
   * Return all files in the path.
   */
  public FileStatus[] listStatus(Path path) throws IOException {
    String tPath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(tPath);
    LOG.debug("listStatus(" + path + "): HDFS Path: " + hdfsPath);
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
    LOG.debug("mkdirs(" + cPath + ", " + permission + ")");

    String path = Utils.getPathWithoutScheme(cPath);
    Path hdfsPath = Utils.getHDFSPath(path);
    FileSystem fs = hdfsPath.getFileSystem(getConf());
    LOG.debug("mkdirs: making dir " + hdfsPath);
    return fs.mkdirs(hdfsPath) && (mTFS.mkdir(path) > 0);
  }

  @Override
  /**
   * Return the inputstream of a file.
   */
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.debug("open(" + cPath + ", " + bufferSize + ")");

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
    LOG.debug("rename(" + src + ", " + dst + ")");
    Path hSrc = Utils.getHDFSPath(src.toString());
    Path hDst = Utils.getHDFSPath(dst.toString());
    FileSystem fs = hSrc.getFileSystem(getConf());
    boolean succeed = false;
    succeed = mTFS.rename(Utils.getPathWithoutScheme(src), Utils.getPathWithoutScheme(dst));
    return fs.rename(hSrc, hDst) && succeed;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    LOG.debug("setWorkingDirectory(" + path + ")");
    if (path.isAbsolute()) {
      mWorkingDir = path;
    } else {
      mWorkingDir = new Path(mWorkingDir, path);
    }
  }
}