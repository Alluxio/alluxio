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
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;

import com.google.common.base.Preconditions;
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
  private final FileSystem mFileSystem;
  private final AuthPolicy mAuthPolicy;
  private final AlluxioURI mURI;
  private final long mMode;
  private final long mOriginalFileLen;

  private FileOutStream mOutStream;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the Alluxio file system
   * @param authPolicy the Authentication policy
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param mode the filesystem mode, -1 if not set
   * @param status the uri status, null if not uri does not exist
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileOutStream create(FileSystem fileSystem, AuthPolicy authPolicy,
      AlluxioURI uri, int flags, long mode, @Nullable URIStatus status) throws Exception {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(authPolicy);
    Preconditions.checkNotNull(uri);
    if (mode == MODE_NOT_SET && status != null) {
      mode = status.getMode();
    }
    long fileLen = status == null ? 0 : status.getLength();
    if (status != null) {
      if (AlluxioFuseOpenUtils.containsTruncate(flags)) {
        fileSystem.delete(uri);
        fileLen = 0;
        LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
            + "Alluxio deleted the old file and created a new file for writing", uri, flags));
      } else {
        // Support open(O_WRONLY flag) - truncate(0) - write() workflow, otherwise error out
        return new FuseFileOutStream(fileSystem, authPolicy, null, fileLen, uri, mode);
      }
    }
    return new FuseFileOutStream(fileSystem, authPolicy,
        AlluxioFuseUtils.createFile(fileSystem, authPolicy, uri, mode), fileLen, uri, mode);
  }

  private FuseFileOutStream(FileSystem fileSystem, AuthPolicy authPolicy,
      @Nullable FileOutStream outStream, long fileLen, AlluxioURI uri, long mode) {
    mFileSystem = fileSystem;
    mAuthPolicy = authPolicy;
    mOutStream = outStream;
    mOriginalFileLen = fileLen;
    mURI = uri;
    mMode = mode;
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
    if (mOutStream == null) {
      throw new IOException(
          "Cannot overwrite/extending existing file without O_TRUNC flag or truncate(0) operation");
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
    if (mOutStream == null) {
      return mOriginalFileLen;
    }
    return mOutStream.getBytesWritten();
  }

  @Override
  public synchronized void flush() throws IOException {
    if (mOutStream == null) {
      return;
    }
    mOutStream.flush();
  }

  @Override
  public synchronized void truncate(long size) throws Exception {
    if (mOutStream != null && mOutStream.getBytesWritten() == size) {
      return;
    }
    if (size == 0) {
      close();
      mFileSystem.delete(mURI);
      mOutStream = AlluxioFuseUtils.createFile(mFileSystem, mAuthPolicy, mURI, mMode);
      return;
    }
    throw new UnsupportedOperationException(
        String.format("Cannot truncate file %s to size %s", mURI, size));
  }

  @Override
  public synchronized void close() throws IOException {
    if (mOutStream != null) {
      mOutStream.close();
    }
  }
}
