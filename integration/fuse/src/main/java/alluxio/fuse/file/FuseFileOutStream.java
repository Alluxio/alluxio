/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.grpc.CreateFilePOptions;
import alluxio.security.authorization.Mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation for {@link FuseFileStream} for sequential write only operations
 * against an Alluxio uri.
 */
@ThreadSafe
public class FuseFileOutStream implements FuseFileStream {
  private static final Logger LOG = LoggerFactory.getLogger(FuseFileOutStream.class);
  private final AuthPolicy mAuthPolicy;
  private final CreateFilePOptions mCreateFileOptions;
  private final FileSystem mFileSystem;
  private final AlluxioURI mURI;

  private FileOutStream mOutStream;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the file system
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param authPolicy the authentication policy
   * @param mode the filesystem mode, -1 if not set
   * @param status the uri status, null if not uri does not exist
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileOutStream create(FileSystem fileSystem, AlluxioURI uri, int flags,
      AuthPolicy authPolicy, long mode, @Nullable URIStatus status) throws Exception {
    if (status != null) {
      if (AlluxioFuseOpenUtils.containsTruncate(flags)) {
        fileSystem.delete(uri);
        LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
            + "Alluxio deleted the old file and created a new file for writing", uri, flags));
      } else {
        throw new IOException(String.format("Failed to create write-only stream for %s: "
            + "cannot overwrite existing file without truncate flag", uri));
      }
    }
    CreateFilePOptions.Builder optionsBuilder = CreateFilePOptions.newBuilder();
    if (mode == MODE_NOT_SET && status != null) {
      optionsBuilder.setMode(new Mode((short) status.getMode()).toProto());
    }
    if (mode != MODE_NOT_SET) {
      optionsBuilder.setMode(new Mode((short) mode).toProto());
    }
    CreateFilePOptions options = optionsBuilder.build();
    FileOutStream out = fileSystem.createFile(uri, options);
    if (authPolicy != null) {
      authPolicy.setUserGroupIfNeeded(uri);
    }
    return new FuseFileOutStream(out, fileSystem, uri, options, authPolicy);
  }

  private FuseFileOutStream(FileOutStream outStream, FileSystem fileSystem,
      AlluxioURI uri, CreateFilePOptions options, AuthPolicy authPolicy) {
    mOutStream = outStream;
    mCreateFileOptions = options;
    mFileSystem = fileSystem;
    mURI = uri;
    mAuthPolicy = authPolicy;
  }

  @Override
  public int read(ByteBuffer buf, long size, long offset) throws IOException {
    throw new UnsupportedOperationException("Cannot read from write only stream");
  }

  @Override
  public synchronized void write(ByteBuffer buf, long size, long offset) throws IOException {
    if (size > Integer.MAX_VALUE) {
      throw new IOException(String.format("Cannot write more than %s", Integer.MAX_VALUE));
    }
    int sz = (int) size;
    long bytesWritten = mOutStream.getBytesWritten();
    if (offset != bytesWritten && offset + sz > bytesWritten) {
      throw new UnsupportedOperationException(String.format("Only sequential write is supported. "
          + "Cannot write bytes of size %s to offset %s when %s bytes have written to path %s",
          size, offset, bytesWritten, mURI));
    }
    if (offset + sz <= bytesWritten) {
      LOG.warn("Skip writing to file {} offset={} size={} when {} bytes has written to file",
          mURI, offset, sz, bytesWritten);
      // To fulfill vim :wq
    }
    final byte[] dest = new byte[sz];
    buf.get(dest, 0, sz);
    mOutStream.write(dest);
  }

  @Override
  public synchronized long getFileLength() {
    return mOutStream.getBytesWritten();
  }

  @Override
  public synchronized void flush() throws IOException {
    mOutStream.flush();
  }

  @Override
  public synchronized void truncate(long size) throws Exception {
    if (mOutStream.getBytesWritten() == size) {
      return;
    }
    if (size == 0) {
      mOutStream.close();
      mFileSystem.delete(mURI);
      mOutStream = mFileSystem.createFile(mURI, mCreateFileOptions);
      if (mAuthPolicy != null) {
        mAuthPolicy.setUserGroupIfNeeded(mURI);
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    mOutStream.close();
  }
}
