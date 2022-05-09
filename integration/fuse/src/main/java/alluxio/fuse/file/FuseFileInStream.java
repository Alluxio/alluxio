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
public class FuseFileInStream implements FuseFileStream {
  private final FileInStream mInStream;

  public static FuseFileInStream create(FileSystem fileSystem, AlluxioURI uri, int flags) throws IOException, AlluxioException {
    if (AlluxioFuseOpenUtils.containsTruncate(flags)) {
      throw new IOException(String.format("Cannot create readonly stream for path %s with flags 0x%x contains truncate", uri, flags));
    }
    // TODO(lu) create utils method for waitForFileCompleted
    URIStatus status;
    try {
      status = fileSystem.getStatus(uri);
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      status = null;
    } catch (Throwable t) {
      throw new IOException(String.format("Failed to create fuse stream for %s, unexpected error when getting file status", uri), t);
    }

    if (status != null && !status.isCompleted()) {
      // Cannot open incomplete file for read or write
      // wait for file to complete in read or read_write mode
      if (!AlluxioFuseUtils.waitForFileCompleted(fileSystem, uri)) {
        throw new IOException(String.format("Failed to create fuse stream for incomplete file %s", uri));
      }
    }

    FileInStream is = fileSystem.openFile(uri);
    return new FuseFileInStream(is);
  }
  
  private FuseFileInStream(FileInStream inStream) {
    mInStream = inStream;
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
