package tachyon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Tachyon stores data into a under layer file system. Any file system implementing
 * this interface can be a valid under layer file system
 */
public abstract class UnderFileSystem {
  public static UnderFileSystem getUnderFileSystem(String path) {
    if (path.startsWith("hdfs://") || path.startsWith("file://")) {
      return UnderFileSystemHdfs.getClient(path);
    }
    CommonUtils.illegalArgumentException("Unknown under file system scheme " + path);
    return null;
  }

  public abstract void close() throws IOException;

  public abstract OutputStream create(String path);

  public abstract void delete(String path, boolean recursive);

  public abstract boolean exist(String src);

  public abstract long getFileSize(String path);

  public abstract boolean mkdirs(String path, boolean createParent);

  public abstract InputStream open(String path);

  public abstract boolean rename(String src, String dst);

  public abstract List<String> getFileLocations(String path);
}
