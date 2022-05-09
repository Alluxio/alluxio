package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.InvalidPathException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class FuseFileInOrOutStream implements FuseFileStream {
  private 

  public static FuseFileInOrOutStream create(FileSystem fileSystem, AlluxioURI uri, int flags, long mode) throws IOException, AlluxioException {
    URIStatus status;
    try {
      status = fileSystem.getStatus(uri);
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      status = null;
    } catch (Throwable t) {
      throw new IOException(String.format("Failed to create fuse stream for %s, unexpected error when getting file status", uri), t);
    }
    boolean truncate = AlluxioFuseOpenUtils.containsTruncate(flags));

  }  
  
  private FuseFileInOrOutStream(FileSystem fileSystem, AlluxioURI uri, long mode) {

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
  public int getFileLength() throws IOException {
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
