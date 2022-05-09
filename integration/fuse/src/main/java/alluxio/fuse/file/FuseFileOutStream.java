package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.grpc.CreateFilePOptions;
import alluxio.security.authorization.Mode;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.InvalidPathException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class FuseFileOutStream implements FuseFileStream {
  private static final Logger LOG = LoggerFactory.getLogger(FuseFileOutStream.class);
  private final FileOutStream mOutStream;

  public static FuseFileOutStream create(FileSystem fileSystem, AlluxioURI uri, int flags, long mode, @Nullable AuthPolicy authPolicy) 
      throws Exception {
    Preconditions.checkNotNull(fileSystem, "file system");
    Preconditions.checkNotNull(uri, "uri");
    if (AlluxioFuseOpenUtils.containsTruncate(flags)) {
      throw new IOException(String.format("Cannot create readonly stream for path %s with flags 0x%x contains truncate", uri, flags));
    }

    URIStatus status;
    try {
      status = fileSystem.getStatus(uri);
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      status = null;
    } catch (Throwable t) {
      throw new IOException(String.format("Failed to create fuse stream for %s, unexpected error when getting file status", uri), t);
    }

    if (status != null) {
      if (AlluxioFuseOpenUtils.containsTruncate(flags)) {
        fileSystem.delete(uri);
        LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
            + "Alluxio deleted the old file and created a new file for writing", uri, flags));
      } else {
        throw new IOException(String.format("Cannot create fuse file output stream for existing file {}. Alluxio does not support overwrite without truncate", uri));
      }
    }

    FileOutStream out = fileSystem.createFile(uri,
        CreateFilePOptions.newBuilder()
            .setMode(new Mode((short) mode).toProto())
            .build());
    if (authPolicy != null) {
      authPolicy.setUserGroupIfNeeded(uri);
    }
    return new FuseFileOutStream(out);
  }
  
  private FuseFileOutStream(FileOutStream outStream) {
    mOutStream = outStream;
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
