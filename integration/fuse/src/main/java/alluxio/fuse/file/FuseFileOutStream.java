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
import java.util.Optional;
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
  // original file len before truncate
  // or the truncated larger file size
  private long mOriginalOrTruncatedFileLen;

  private Optional<FileOutStream> mOutStream;

  /**
   * Creates a {@link FuseFileInOrOutStream}.
   *
   * @param fileSystem the Alluxio file system
   * @param authPolicy the Authentication policy
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param mode the filesystem mode, -1 if not set
   * @param status the uri status
   * @return a {@link FuseFileInOrOutStream}
   */
  public static FuseFileOutStream create(FileSystem fileSystem, AuthPolicy authPolicy,
      AlluxioURI uri, int flags, long mode, Optional<URIStatus> status) {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(authPolicy);
    Preconditions.checkNotNull(uri);
    Preconditions.checkNotNull(status);
    if (mode == AlluxioFuseUtils.MODE_NOT_SET_VALUE && status.isPresent()) {
      mode = status.get().getMode();
    }
    long fileLen = status.map(URIStatus::getLength).orElse(0L);
    if (status.isPresent()) {
      if (AlluxioFuseOpenUtils.containsTruncate(flags) || fileLen == 0) {
        // support create file then open with truncate flag to write workload
        // support create empty file then open for write/read_write workload
        AlluxioFuseUtils.deleteFile(fileSystem, uri);
        fileLen = 0;
        LOG.debug(String.format("Open path %s with flag 0x%x for overwriting. "
            + "Alluxio deleted the old file and created a new file for writing", uri, flags));
      } else {
        // Support open(O_WRONLY flag) - truncate(0) - write() workflow, otherwise error out
        return new FuseFileOutStream(fileSystem, authPolicy, Optional.empty(), fileLen, uri, mode);
      }
    }
    return new FuseFileOutStream(fileSystem, authPolicy,
        Optional.of(AlluxioFuseUtils.createFile(fileSystem, authPolicy, uri, mode)),
        fileLen, uri, mode);
  }

  private FuseFileOutStream(FileSystem fileSystem, AuthPolicy authPolicy,
      Optional<FileOutStream> outStream, long fileLen, AlluxioURI uri, long mode) {
    mFileSystem = Preconditions.checkNotNull(fileSystem);
    mAuthPolicy = Preconditions.checkNotNull(authPolicy);
    mOutStream = Preconditions.checkNotNull(outStream);
    mURI = Preconditions.checkNotNull(uri);
    mOriginalOrTruncatedFileLen = fileLen;
    mMode = mode;
  }

  @Override
  public int read(ByteBuffer buf, long size, long offset) {
    throw new UnsupportedOperationException("Cannot read from write only stream");
  }

  @Override
  public synchronized void write(ByteBuffer buf, long size, long offset) {
    if (size > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          String.format("Cannot write more than %s", Integer.MAX_VALUE));
    }
    if (!mOutStream.isPresent()) {
      throw new UnsupportedOperationException(
          "Cannot overwrite/extending existing file without O_TRUNC flag or truncate(0) operation");
    }
    int sz = (int) size;
    long bytesWritten = mOutStream.get().getBytesWritten();
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
    try {
      mOutStream.get().write(dest);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized long getFileLength() {
    return mOutStream.map(fileOutStream -> Math.max(fileOutStream.getBytesWritten(), mOriginalOrTruncatedFileLen))
        .orElseGet(() -> mOriginalOrTruncatedFileLen);
  }

  @Override
  public synchronized void flush() {
    if (!mOutStream.isPresent()) {
      return;
    }
    try {
      mOutStream.get().flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void truncate(long size) {
    if (mOutStream.isPresent() && mOutStream.get().getBytesWritten() == size) {
      return;
    }
    if (size == 0) {
      close();
      AlluxioFuseUtils.deleteFile(mFileSystem, mURI);
      mOriginalOrTruncatedFileLen = 0;
      mOutStream = Optional.of(AlluxioFuseUtils.createFile(mFileSystem, mAuthPolicy, mURI, mMode));
      return;
    }
    if (size > getFileLength()) {
      LOG.warn("Truncate file {} to size {} which is larger than the written bytes {}. "
          + "Note that Alluxio does not support set file size and will treat this operation as noop",
          mURI, size, getFileLength());
      mOriginalOrTruncatedFileLen = size;
      return;
    }
    throw new UnsupportedOperationException(
        String.format("Cannot truncate file %s to size %s", mURI, size));
  }

  @Override
  public synchronized void close() {
    if (mOutStream.isPresent()) {
      try {
        long bytesWritten = mOutStream.get().getBytesWritten();
        if (mOriginalOrTruncatedFileLen != 0 && mOriginalOrTruncatedFileLen != bytesWritten) {
          LOG.warn("Truncated file {} to size {} but the actual written size is {}. "
              + "Alluxio does not support truncate to size larger than bytes written",
              mURI, mOriginalOrTruncatedFileLen, bytesWritten));
        }
        mOutStream.get().close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
