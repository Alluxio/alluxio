package tachyon;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Single node UnderFilesystem implementation.
 * 
 * This only works for single machine. It is for local unit test and single machine mode.
 * 
 * TODO Make it work for multiple machines.
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void delete(String path, boolean recursive) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean exist(String src) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }
}
