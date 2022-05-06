package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FuseFileInOrOutStream implements FuseFileStream {
  

  public FuseFileInOrOutStream(FileSystem fileSystem, AlluxioURI uri, long mode) {

  }
  
  @Override
  public int read(ByteBuffer buf, long size, long offset) throws IOException {
    return 0;
  }

  @Override
  public int write(ByteBuffer buf, long size, long offset) throws IOException {
    return 0;
  }

  @Override
  public int flush() throws IOException {
    return 0;
  }

  @Override
  public int truncate(long size) throws IOException {
    return 0;
  }

  @Override
  public int close() throws IOException {
    return 0;
  }
}
