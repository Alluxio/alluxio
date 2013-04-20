package tachyon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Tachyon stores data into an under layer file system. Any file system implementing
 * this interface can be a valid under layer file system
 */
public abstract class UnderFileSystem {
  public static UnderFileSystem getUnderFileSystem(String path) {
    if (path.startsWith("hdfs://") || path.startsWith("file://") || path.startsWith("s3://")) {
      return UnderFileSystemHdfs.getClient(path);
    } else if (path.startsWith("/")) {
      return UnderFileSystemSingleLocal.getClient();
    }
    CommonUtils.illegalArgumentException("Unknown under file system scheme " + path);
    return null;
  }

  public abstract void close() throws IOException;

  public abstract OutputStream create(String path) throws IOException;

  public abstract boolean delete(String path, boolean recursive) throws IOException;

  public abstract boolean exists(String path) throws IOException;

  public abstract List<String> getFileLocations(String path) throws IOException;

  public abstract long getFileSize(String path) throws IOException;

  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  public abstract InputStream open(String path) throws IOException;

  public abstract boolean rename(String src, String dst) throws IOException;
}
