package tachyon;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a HDFS Client for TRex. It handles all sorts of retry logic.
 * @author haoyuan
 */
public class HdfsClient {
  private static final int MAX_TRY = 5; 
  private final Logger LOG = LoggerFactory.getLogger(HdfsClient.class);

  private FileSystem mFs = null;

  public HdfsClient(String fsDefaultName) {
    try {
      Configuration tConf = new Configuration();
      tConf.set("fs.default.name", fsDefaultName);
      mFs = FileSystem.get(tConf);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite, String src, String dst) {
    IOException te = null;
    LOG.info("Trying to copy from " + src + " to " + dst);
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        mFs.copyFromLocalFile(delSrc, overwrite, new Path(src), new Path(dst));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
      LOG.info("Finished the copy from " + src + " to " + dst);
      return;
    }
    CommonUtils.runtimeException(te);
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        mFs.copyToLocalFile(delSrc, src, dst);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
      return;
    }
    CommonUtils.runtimeException(te);
  }

  public void delete(Path f, boolean recursive) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        mFs.delete(f, recursive);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
      return;
    }
    CommonUtils.runtimeException(te);
  }

  public boolean mkdirs(String src, FsPermission permission, boolean createParent) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        if (mFs.exists(new Path(src))) {
          return true;
        }
        return mFs.mkdirs(new Path(src), permission);
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

  public boolean rename(String src, String dst) {
    IOException te = null;
    int cnt = 0;
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