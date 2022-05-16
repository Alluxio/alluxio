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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.fuse.AlluxioFuseOpenUtils;
import alluxio.fuse.AlluxioFuseUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation for {@link FuseFileStream} for read only operations against an Alluxio uri.
 */
@ThreadSafe
public class FuseFileInStream implements FuseFileStream {
  private final FileInStream mInStream;
  private final long mFileLength;
  private final AlluxioURI mURI;

  /**
   * Creates a {@link FuseFileInStream}.
   *
   * @param fileSystem the file system
   * @param uri the alluxio uri
   * @param flags the fuse create/open flags
   * @param status the uri status, null if not uri does not exist
   * @return a {@link FuseFileInStream}
   */
  public static FuseFileInStream create(FileSystem fileSystem, AlluxioURI uri,
      int flags, @Nullable URIStatus status) throws IOException, AlluxioException {
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(uri);
    if (AlluxioFuseOpenUtils.containsTruncate(flags)) {
      throw new IOException(String.format(
          "Failed to create read-only stream for path %s: flags 0x%x contains truncate",
          uri, flags));
    }
    if (status == null) {
      throw new IOException(String.format(
          "Failed to create read-only stream for %s: file does not exist", uri));
    }

    if (!status.isCompleted()) {
      // Cannot open incomplete file for read
      // wait for file to complete in read or read_write mode
      if (!AlluxioFuseUtils.waitForFileCompleted(fileSystem, uri)) {
        throw new IOException(String.format(
            "Failed to create read-only stream for %s: incomplete file", uri));
      }
    }

    FileInStream is = fileSystem.openFile(uri);
    return new FuseFileInStream(is, status.getLength(), uri);
  }

  private FuseFileInStream(FileInStream inStream, long fileLength, AlluxioURI uri) {
    mInStream = inStream;
    mFileLength = fileLength;
    mURI = uri;
  }

  @Override
  public synchronized int read(ByteBuffer buf, long size, long offset) throws IOException {
    final int sz = (int) size;
    int nread = 0;
    int rd = 0;
    if (offset - mInStream.getPos() >= mInStream.remaining()) {
      return 0;
    }
    mInStream.seek(offset);
    while (rd >= 0 && nread < sz) {
      rd = mInStream.read(buf, nread, sz - nread);
      if (rd >= 0) {
        nread += rd;
      }
    }
    return nread;
  }

  @Override
  public void write(ByteBuffer buf, long size, long offset) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(String
        .format("Cannot write to read-only stream of path %s", mURI));
  }

  @Override
  public long getFileLength() {
    return mFileLength;
  }

  @Override
  public void flush() {}

  @Override
  public void truncate(long size) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(String
        .format("Cannot truncate read-only stream of path %s", mURI));
  }

  @Override
  public synchronized void close() throws IOException {
    mInStream.close();
  }
}
