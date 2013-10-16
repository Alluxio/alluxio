package tachyon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Single node UnderFilesystem implementation.
 *
 * This only works for single machine. It is for local unit test and single machine mode.
 */
public class UnderFileSystemSingleLocal extends UnderFileSystem {

  public static UnderFileSystem getClient() {
    return new UnderFileSystemSingleLocal();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return new FileOutputStream(path);
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path, (short) 1, blockSizeByte);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    if (replication != 1) {
      throw new IOException("UnderFileSystemSingleLocal does not provide more than one" +
          " replication factor");
    }
    return new FileOutputStream(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    File file = new File(path);
    boolean success = true;
    if (recursive && file.isDirectory()) {
      String[] files = file.list();
      for (String child : files) {
        success = success && delete(path + "/" + child, true);
      }
    }

    return success && file.delete();
  }

  @Override
  public boolean exists(String path) throws IOException {
    File file = new File(path);
    return file.exists();
  }

  @Override
  public String[] getChildren(String path) throws IOException {
    File file = new File(path);
    File[] files = file.listFiles();
    String[] rtn = new String[files.length];
    int i = 0;
    for (File f : files) {
      rtn[i ++] = f.getAbsolutePath();
    }
    return rtn;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    List<String> ret = new ArrayList<String>();
    ret.add(InetAddress.getLocalHost().getCanonicalHostName());
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    return getFileLocations(path);
  }

  @Override
  public long getFileSize(String path) throws IOException {
    File file = new File(path);
    return file.length();
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    File file = new File(path);
    if (!file.exists()) {
      throw new FileNotFoundException(path);
    }
    return Constants.GB * 2L;
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    File file = new File(path);
    switch (type) {
    case SPACE_TOTAL:
      return file.getTotalSpace();
    case SPACE_FREE:
      return file.getFreeSpace();
    case SPACE_USED:
      return file.getTotalSpace() - file.getFreeSpace();
    }
    throw new IOException("Unknown getSpace parameter: " + type);
  }

  @Override
  public boolean isFile(String path) throws IOException {
    File file = new File(path);
    return file.isFile();
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    File file = new File(path);
    return createParent ? file.mkdirs(): file.mkdir();
  }

  @Override
  public InputStream open(String path) throws IOException {
    return new FileInputStream(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    File file = new File(src);
    return file.renameTo(new File(dst));
  }
}
