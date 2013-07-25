package tachyon;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * HDFS UnderFilesystem implementation.
 */
public class UnderFileSystemHdfs extends UnderFileSystem {
  private static final int MAX_TRY = 5; 
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private FileSystem mFs = null;

  public static UnderFileSystemHdfs getClient(String path) {
    return new UnderFileSystemHdfs(path);
  }

  private UnderFileSystemHdfs(String fsDefaultName) {
    try {
      Configuration tConf = new Configuration();
      tConf.set("fs.defaultFS", fsDefaultName);
      tConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      Path path = new Path(fsDefaultName);
      mFs = path.getFileSystem(tConf);
      // FileSystem.get(tConf);
      // mFs = FileSystem.get(new URI(fsDefaultName), tConf);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    mFs.close();
  }

  @Override
  public FSDataOutputStream create(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.create(new Path(path));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    throw te;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("deleting " + path + " " + recursive);
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.delete(new Path(path), recursive);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    throw te;
  }

  @Override
  public boolean exists(String path) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.exists(new Path(path));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return false;
  }

  @Override
  public List<String> getFileLocations(String path) {
    return getFileLocations(path, 0);
  }

  @Override
  public List<String> getFileLocations(String path, long offset) {
    List<String> ret = new ArrayList<String>();
    try {
      FileStatus fStatus = mFs.getFileStatus(new Path(path));
      BlockLocation[] bLocations = mFs.getFileBlockLocations(fStatus, offset, 1);
      if (bLocations.length > 0) {
        String[] hosts = bLocations[0].getHosts();
        for (String host: hosts) {
          ret.add(host);
        }
      }
    } catch (IOException e) {
      LOG.error(e);
    }
    return ret;
  }

  @Override
  public long getFileSize(String path) {
    int cnt = 0;
    Path tPath = new Path(path);
    while (cnt < MAX_TRY) {
      try {
        FileStatus fs = mFs.getFileStatus(tPath);
        return fs.getLen();
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        continue;
      }
    }
    return -1;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFs.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFs.getFileStatus(tPath);
    return fs.getBlockSize();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // TODO Hadoop 1.x does not provide user API to get this. Hadoop 2.x provides.
    // Use JAVA reflection to get the space info for Hadoop 2.x
    return -1;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        if (mFs.exists(new Path(path))) {
          return true;
        }
        return mFs.mkdirs(new Path(path), null);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return false;
  }

  @Override
  public FSDataInputStream open(String path) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.open(new Path(path));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return null;
  }

  @Override
  public boolean rename(String src, String dst) {
    IOException te = null;
    int cnt = 0;
    LOG.debug("Renaming from " + src + " to " + dst);
    if (!exists(src)) {
      LOG.error("File " + src + " does not exist. Therefore rename to " + dst + " failed.");
    }

    if (exists(dst)) {
      LOG.error("File " + dst + " does exist. Therefore rename from " + src + " failed.");
    }

    while (cnt < MAX_TRY) {
      try {
        return mFs.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return false;
  }
}