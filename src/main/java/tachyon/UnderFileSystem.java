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
  public enum SpaceType {
    SPACE_TOTAL(0),
    SPACE_FREE(1),
    SPACE_USED(2);

    private final int value;

    private SpaceType(int value) {
      this.value = value;
    }

    /**
     * Get the integer value of this enum value.
     */
    public int getValue() {
      return value;
    }
  }

  public static UnderFileSystem get(String path) {
    if (path.startsWith("hdfs://") || path.startsWith("file://") ||
        path.startsWith("s3://") || path.startsWith("s3n://")) {
      return UnderFileSystemHdfs.getClient(path);
    } else if (path.startsWith("/")) {
      return UnderFileSystemSingleLocal.getClient();
    }
    CommonUtils.illegalArgumentException("Unknown under file system scheme " + path);
    return null;
  }

  public abstract void close() throws IOException;

  public abstract OutputStream create(String path) throws IOException;

  public abstract OutputStream create(String path, int blockSizeByte) throws IOException;

  public abstract OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException;

  public abstract boolean delete(String path, boolean recursive) throws IOException;

  public abstract boolean exists(String path) throws IOException;

  public abstract List<String> getFileLocations(String path) throws IOException;

  public abstract List<String> getFileLocations(String path, long offset) throws IOException;

  public abstract long getFileSize(String path) throws IOException;

  public abstract long getBlockSizeByte(String path) throws IOException;

  public abstract long getSpace(String path, SpaceType type) throws IOException;

  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  public abstract InputStream open(String path) throws IOException;

  public abstract boolean rename(String src, String dst) throws IOException;
}
