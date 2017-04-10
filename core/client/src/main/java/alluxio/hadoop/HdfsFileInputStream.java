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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An input stream for reading a file from HDFS.
 */
@NotThreadSafe
public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileInputStream.class);

  private final Statistics mStatistics;
  private final URIStatus mFileInfo;
  private final FileInStream mInputStream;

  private boolean mClosed = false;

  private long mCurrentPosition;

  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param context the file system context
   * @param uri the Alluxio file URI
   * @param stats filesystem statistics
   * @throws IOException if the underlying file does not exist or its stream cannot be created
   */
  public HdfsFileInputStream(FileSystemContext context, AlluxioURI uri,
      org.apache.hadoop.fs.FileSystem.Statistics stats) throws IOException {
    LOG.debug("HdfsFileInputStream({}, {})", uri, stats);

    mCurrentPosition = 0;
    mStatistics = stats;
    FileSystem fs = FileSystem.Factory.get(context);
    try {
      mFileInfo = fs.getStatus(uri);
      mInputStream = fs.openFile(uri, OpenFileOptions.defaults());
    } catch (FileDoesNotExistException e) {
      // Transform the Alluxio exception to a Java exception to satisfy the HDFS API contract.
      throw new FileNotFoundException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(uri));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int available() throws IOException {
    if (mClosed) {
      throw new IOException("Cannot query available bytes from a closed stream.");
    }
    return (int) mInputStream.remaining();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mInputStream.close();
    mClosed = true;
  }

  @Override
  public long getPos() throws IOException {
    return mCurrentPosition;
  }

  @Override
  public int read() throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }

    int ret = mInputStream.read();
    if (ret != -1) {
      mCurrentPosition++;
      if (mStatistics != null) {
        mStatistics.incrementBytesRead(1);
      }
    }
    return ret;
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }

    int ret = mInputStream.read(buffer, offset, length);
    if (ret != -1) {
      mCurrentPosition += ret;
      if (mStatistics != null) {
        mStatistics.incrementBytesRead(ret);
      }
    }
    return ret;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }

    int bytesRead = mInputStream.positionedRead(position, buffer, offset, length);
    if (mStatistics != null && bytesRead != -1) {
      mStatistics.incrementBytesRead(bytesRead);
    }
    return bytesRead;
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int n = 0; // total bytes read
    while (n < length) {
      int ret = read(position + n, buffer, offset + n, length - n);
      if (ret == -1) {
        throw new EOFException();
      }
      n += ret;
    }
  }

  /**
   * Seek to the given offset from the start of the file. The next {@link #read()} will be from that
   * location. Can't seek past the end of the file.
   *
   * @param pos the position to seek to
   * @throws IOException if the position is negative or exceeds the end of the file
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos == mCurrentPosition) {
      return;
    }

    if (pos < 0) {
      throw new IOException(ExceptionMessage.SEEK_NEGATIVE.getMessage(pos));
    }
    if (pos > mFileInfo.getLength()) {
      throw new IOException(ExceptionMessage.SEEK_PAST_EOF.getMessage(pos, mFileInfo.getLength()));
    }

    mInputStream.seek(pos);
    mCurrentPosition = pos;
  }

  /**
   * This method is not supported in {@link HdfsFileInputStream}.
   *
   * @param targetPos N/A
   * @return N/A
   * @throws IOException always
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
  }

  /**
   * Skips over the given bytes from the current position. Since the inherited
   * {@link InputStream#skip(long)} is inefficient, {@link #skip(long)} should be explicitly
   * overrided.
   *
   * @param n the number of bytes to be skipped
   * @return the actual number of bytes skipped
   * @throws IOException if the after position exceeds the end of the file
   */
  @Override
  public long skip(long n) throws IOException {
    if (mClosed) {
      throw new IOException("Cannot skip bytes in a closed stream.");
    }
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, available());
    seek(mCurrentPosition + toSkip);
    return toSkip;
  }
}
