package tachyon.hadoop;

import java.io.FileNotFoundException;
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
import tachyon.PrefixList;
import tachyon.UnderfsUtil;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;

/**
 * An Hadoop FileSystem interface implementation. Any program working with Hadoop HDFS can work
 * with Tachyon transparently by using this class. However, it is not as efficient as using
 * the Tachyon API in tachyon.client package.
 */
public class TFS extends FileSystem {
  public static final  String FIRST_COM_PATH = "tachyon_dep/";
  public static final  String RECOMPUTE_PATH = "tachyon_recompute/";

  public static String UNDERFS_ADDRESS;

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private URI mUri = null;
  private Path mWorkingDir = new Path("/");
  private TachyonFS mTFS = null;
  private String mTachyonHeader = null;

  private void fromHdfsToTachyon(String path) throws IOException {
    if (!mTFS.exist(path)) {
      Path hdfsPath = Utils.getHDFSPath(path);
      FileSystem fs = hdfsPath.getFileSystem(getConf());
      if (fs.exists(hdfsPath)) {
        UnderfsUtil.getInfo(mTFS, UNDERFS_ADDRESS, path, new PrefixList(null));
      }
    }
  }

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

    if (cPath.toString().contains(FIRST_COM_PATH) && !cPath.toString().contains("SUCCESS")) {
      String path = Utils.getPathWithoutScheme(cPath);
      mTFS.createFile(path, blockSize);
      path = path.substring(path.indexOf(FIRST_COM_PATH) + FIRST_COM_PATH.length());
      path = path.substring(0, path.indexOf("/"));
      int depId = Integer.parseInt(path);
      LOG.info("create(" + cPath + ") : " + path + " " + depId);
      path = Utils.getPathWithoutScheme(cPath);
      path = path.substring(path.indexOf("part-") + 5);
      int index = Integer.parseInt(path);
      ClientDependencyInfo info = mTFS.getClientDependencyInfo(depId);
      int fileId = info.getChildren().get(index);
      LOG.info("create(" + cPath + ") : " + path + " " + index + " " + info + " " + fileId);

      TachyonFile file = mTFS.getFile(fileId);
//      if (file.getBlockSizeByte() != blockSize) {
//        throw new IOException("File already exist with a different blocksize " +
//            file.getBlockSizeByte() + " != " + blockSize);
//      }
      return new FSDataOutputStream(file.getOutStream(WriteType.MUST_CACHE), null);
    } if (cPath.toString().contains(RECOMPUTE_PATH) && !cPath.toString().contains("SUCCESS")) {
      String path = Utils.getPathWithoutScheme(cPath);
      path = path.substring(path.indexOf(RECOMPUTE_PATH) + RECOMPUTE_PATH.length());
      path = path.substring(0, path.indexOf("/"));
      int depId = Integer.parseInt(path);
      LOG.info("create(" + cPath + ") : " + path + " " + depId);
      path = Utils.getPathWithoutScheme(cPath);
      path = path.substring(path.indexOf("part-") + 5);
      int index = Integer.parseInt(path);
      ClientDependencyInfo info = mTFS.getClientDependencyInfo(depId);
      int fileId = info.getChildren().get(index);
      LOG.info("create(" + cPath + ") : " + path + " " + index + " " + info + " " + fileId);

      TachyonFile file = mTFS.getFile(fileId);
//      if (file.getBlockSizeByte() != blockSize) {
//        throw new IOException("File already exist with a different blocksize " +
//            file.getBlockSizeByte() + " != " + blockSize);
//      }
      return new FSDataOutputStream(file.getOutStream(WriteType.MUST_CACHE), null);
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
//      if (file.getBlockSizeByte() != blockSize) {
//        throw new IOException("File already exist with a different blocksize " +
//            file.getBlockSizeByte() + " != " + blockSize);
//      }
      return new FSDataOutputStream(file.getOutStream(type), null);
    }
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

  @Override
  /**
   * Return the status of a single file.
   * 
   * If the file does not exist in Tachyon, query it from HDFS. 
   */
  public FileStatus getFileStatus(Path path) throws IOException {
    String tPath = Utils.getPathWithoutScheme(path);
    Path hdfsPath = Utils.getHDFSPath(tPath);

    LOG.info("getFileStatus(" + path + "): HDFS Path: " + hdfsPath + " TPath: " +
        mTachyonHeader + tPath);

    fromHdfsToTachyon(tPath);
    TachyonFile file = mTFS.getFile(tPath);
    if (file == null) {
      LOG.info("File does not exist: " + path);
      throw new FileNotFoundException("File does not exist: " + path);
    }

    FileStatus ret = new FileStatus(file.length(), file.isDirectory(), file.getDiskReplication(),
        file.getBlockSizeByte(), file.getCreationTimeMs(), file.getCreationTimeMs(),
        null, null, null, new Path(mTachyonHeader + tPath));
    return ret;
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

    BlockLocation[] ret = new BlockLocation[blockLocations.size()];
    for (int k = 0; k < blockLocations.size(); k ++) {
      ret[k] = blockLocations.get(k);
    }
    return ret;
  }

  /**
   * Initialize the class, have a lazy connection with Tachyon through mTFS.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    LOG.info("initialize(" + uri + ", " + conf + "). Connecting to Tachyon: " + uri.toString());
    mTFS = TachyonFS.get(new InetSocketAddress(uri.getHost(), uri.getPort()));
    mTachyonHeader = "tachyon://" + uri.getHost() + ":" + uri.getPort();
    mUri = URI.create(mTachyonHeader);
    UNDERFS_ADDRESS = mTFS.getUnderfsAddress();
    LOG.info(mTachyonHeader + " " + mUri + " " + UNDERFS_ADDRESS);
  }

  @Override
  /**
   * List entries of a path
   */
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
      ret[k] = new FileStatus(info.getLength(), info.isFolder(), 3, info.getBlockSizeByte(),
          info.getCreationTimeMs(), info.getCreationTimeMs(), 
          null, null, null, new Path(mTachyonHeader + info.getPath()));
    }
    return ret;
  }

  @Override
  public boolean mkdirs(Path cPath, FsPermission permission) throws IOException  {
    LOG.info("mkdirs(" + cPath + ", " + permission + ") ");
    return mTFS.mkdir(Utils.getPathWithoutScheme(cPath));
  }

  @Override
  /**
   * Return the FSDataInputStream of a file.
   */
  public FSDataInputStream open(Path cPath, int bufferSize) throws IOException {
    LOG.info("open(" + cPath + ", " + bufferSize + ")");

    String path = Utils.getPathWithoutScheme(cPath);
    fromHdfsToTachyon(path);
    int fileId = mTFS.getFileId(path);

    return new FSDataInputStream(new HdfsFileInputStream(mTFS, fileId,
        Utils.getHDFSPath(path), getConf(), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("rename(" + src + ", " + dst + ")");
    String hSrc = Utils.getPathWithoutScheme(src);
    String hDst = Utils.getPathWithoutScheme(dst);
    fromHdfsToTachyon(hSrc);
    return mTFS.rename(hSrc, hDst);
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
}