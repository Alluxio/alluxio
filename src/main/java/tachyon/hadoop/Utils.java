package tachyon.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.log4j.Logger;

import tachyon.Constants;

public final class Utils {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  public static String HDFS_ADDRESS;
  private static final boolean DEBUG = Constants.DEBUG;

  public static String getTachyonFileName(String path) {
    while (path.contains(":")) {
      int index = path.indexOf(":");
      path = path.substring(index + 1);
    }

    while (!path.startsWith("/")) {
      path = path.substring(1);
    }

    return path;
  }

  public static Path getHDFSPath(String path) {
    path = getTachyonFileName(path);

    String mid = "/";
    if (path.startsWith("/")) {
      mid = "";
    }

    return new Path(HDFS_ADDRESS + mid + path);
  }

  public static String getPathWithoutScheme(Path path) {
    Path ori = path;
    String ret = "";
    while (path != null) {
      if (ret.equals("")) {
        ret = path.getName();
      } else {
        ret = path.getName() + "/" + ret;
      }
      path = path.getParent();
    }
    if (DEBUG) {
      LOG.info("Utils getPathWithoutScheme(" + ori + ") result: " + ret);
    }
    return ret;
  }

  public static String toStringHadoopFileStatus(FileStatus fs) {
    StringBuilder sb = new StringBuilder();
    sb.append("HadoopFileStatus: Path: ").append(fs.getPath());
    sb.append(" , Length: ").append(fs.getLen());
    sb.append(" , IsDir: ").append(fs.isDir());
    sb.append(" , BlockReplication: ").append(fs.getReplication());
    sb.append(" , BlockSize: ").append(fs.getBlockSize());
    sb.append(" , ModificationTime: ").append(fs.getModificationTime());
    sb.append(" , AccessTime: ").append(fs.getAccessTime());
    sb.append(" , Permission: ").append(fs.getPermission());
    sb.append(" , Owner: ").append(fs.getOwner());
    sb.append(" , Group: ").append(fs.getGroup());
    return sb.toString();
  }

  public static String toStringHadoopFileSplit(FileSplit fs) {
    StringBuilder sb = new StringBuilder();
    sb.append("HadoopFileSplit: Path: ").append(fs.getPath());
    sb.append(" , Start: ").append(fs.getStart());
    sb.append(" , Length: ").append(fs.getLength());
    sb.append(" , Hosts: ");
    String[] locs;
    try {
      locs = fs.getLocations();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      locs = new String[]{};
    }
    for (String loc: locs) {
      sb.append(loc).append("; ");
    }

    return sb.toString();
  }

  public static String toStringHadoopInputSplit(InputSplit is) {
    StringBuilder sb = new StringBuilder("HadoopInputSplit: ");
    try {
      sb.append(" Length: ").append(is.getLength());
      sb.append(" , Locations: ");
      for (String loc: is.getLocations()) {
        sb.append(loc).append(" ; ");
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return sb.toString();
  }
}