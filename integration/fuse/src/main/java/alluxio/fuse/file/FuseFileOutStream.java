package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.grpc.CreateFilePOptions;
import alluxio.security.authorization.Mode;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

public class FuseFileOutStream implements FuseFileStream {
  private final FileOutStream mOut;

  public static FuseFileOutStream create(FileSystem fileSystem, AlluxioURI uri, long mode, @Nullable AuthPolicy authPolicy) 
      throws Exception {
    Preconditions.checkNotNull(fileSystem, "file system");
    Preconditions.checkNotNull(uri, "uri");
    FileOutStream out = fileSystem.createFile(uri,
        CreateFilePOptions.newBuilder()
            .setMode(new Mode((short) mode).toProto())
            .build());
    if (authPolicy != null) {
      authPolicy.setUserGroupIfNeeded(uri);
    }
    return new FuseFileOutStream(out);
  }
  
  private FuseFileOutStream(FileOutStream out) {
    mOut = out;
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
