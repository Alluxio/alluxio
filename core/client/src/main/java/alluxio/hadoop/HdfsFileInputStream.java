/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.util.io.BufferUtils;

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private long mCurrentPosition;
  private Path mHdfsPath;
  private org.apache.hadoop.conf.Configuration mHadoopConf;
  private int mHadoopBufferSize;
  private Statistics mStatistics;
  private URIStatus mFileInfo;

  private FSDataInputStream mHdfsInputStream = null;

  private FileInStream mAlluxioFileInputStream = null;

  private boolean mClosed = false;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte[] mBuffer;

  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param uri the Alluxio file URI
   * @param conf Hadoop configuration
   * @param bufferSize the buffer size
   * @param stats filesystem statistics
   * @throws IOException if the underlying file does not exist or its stream cannot be created
   */
  public HdfsFileInputStream(AlluxioURI uri, org.apache.hadoop.conf.Configuration conf,
      int bufferSize, org.apache.hadoop.fs.FileSystem.Statistics stats) throws IOException {
    LOG.debug("HdfsFileInputStream({}, {}, {}, {}, {})", uri, conf, bufferSize, stats);
    Configuration configuration = ClientContext.getConf();
    long bufferBytes = configuration.getBytes(Constants.USER_FILE_BUFFER_BYTES);
    mBuffer = new byte[Ints.checkedCast(bufferBytes) * 4];
    mCurrentPosition = 0;
    FileSystem fs = FileSystem.Factory.get();
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;
    mStatistics = stats;
    try {
      mFileInfo = fs.getStatus(uri);
      mHdfsPath = new Path(mFileInfo.getUfsPath());
      mAlluxioFileInputStream =
          fs.openFile(uri, OpenFileOptions.defaults().setReadType(ReadType.CACHE));
    } catch (FileDoesNotExistException e) {
      throw new FileNotFoundException(
          ExceptionMessage.HDFS_FILE_NOT_FOUND.getMessage(mHdfsPath, uri));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * This method is not supported in {@link HdfsFileInputStream}.
   *
   * @return N/A
   * @throws IOException always
   */
  @Override
  public int available() throws IOException {
    throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
  }

  @Override
  public void close() throws IOException {
    if (mAlluxioFileInputStream != null) {
      mAlluxioFileInputStream.close();
    }
    if (mHdfsInputStream != null) {
      mHdfsInputStream.close();
    }
    mClosed = true;
  }

  /**
   * Sets {@link #mHdfsInputStream} to a stream from the under storage system with the stream
   * starting at {@link #mCurrentPosition}.
   *
   * @throws IOException if opening the file fails
   */
  // TODO(calvin): Consider removing this when the recovery logic is available in FileInStream
  private void getHdfsInputStream() throws IOException {
    if (mHdfsInputStream == null) {
      org.apache.hadoop.fs.FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
      mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
      mHdfsInputStream.seek(mCurrentPosition);
    }
  }

  /**
   * Sets {@link #mHdfsInputStream} to a stream from the under storage system with the stream
   * starting at position. The {@link #mCurrentPosition} is not modified to be position.
   *
   * @throws IOException if opening the file fails
   */
  private void getHdfsInputStream(long position) throws IOException {
    if (mHdfsInputStream == null) {
      org.apache.hadoop.fs.FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
      mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
    }
    mHdfsInputStream.seek(position);
  }

  @Override
  public long getPos() throws IOException {
    return mCurrentPosition;
  }

  @Override
  public int read() throws IOException {
    if (mClosed) {
      throw new IOException("Cannot read from a closed stream.");
    }
    if (mAlluxioFileInputStream != null) {
      int ret = 0;
      try {
        ret = mAlluxioFileInputStream.read();
        if (mStatistics != null && ret != -1) {
          mStatistics.incrementBytesRead(1);
        }
        mCurrentPosition++;
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mAlluxioFileInputStream.close();
        mAlluxioFileInputStream = null;
      }
    }
    getHdfsInputStream();
    return readFromHdfsBuffer();
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException("Cannot read from a closed stream.");
    }
    if (mAlluxioFileInputStream != null) {
      int ret = 0;
      try {
        ret = mAlluxioFileInputStream.read(buffer, offset, length);
        if (mStatistics != null && ret != -1) {
          mStatistics.incrementBytesRead(ret);
        }
        mCurrentPosition += ret;
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mAlluxioFileInputStream.close();
        mAlluxioFileInputStream = null;
      }
    }

    getHdfsInputStream();
    int byteRead = readFromHdfsBuffer();
    // byteRead is an unsigned byte, if its -1 then we have hit EOF
    if (byteRead == -1) {
      return -1;
    }
    // Convert byteRead back to a signed byte
    buffer[offset] = (byte) byteRead;
    return 1;
  }

  @Override
  public synchronized int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
    }
    int ret = -1;
    long oldPos = getPos();
    if ((position < 0) || (position >= mFileInfo.getLength())) {
      return ret;
    }

    if (mAlluxioFileInputStream != null) {
      try {
        mAlluxioFileInputStream.seek(position);
        ret = mAlluxioFileInputStream.read(buffer, offset, length);
        if (mStatistics != null && ret != -1) {
          mStatistics.incrementBytesRead(ret);
        }
        return ret;
      } finally {
        mAlluxioFileInputStream.seek(oldPos);
      }
    }

    try {
      getHdfsInputStream(position);
      ret = mHdfsInputStream.read(buffer, offset, length);
      if (mStatistics != null && ret != -1) {
        mStatistics.incrementBytesRead(ret);
      }
      return ret;
    } finally {
      if (mHdfsInputStream != null) {
        mHdfsInputStream.seek(oldPos);
      }
    }
  }

  /**
   * Similar to read(), returns a single unsigned byte from the hdfs buffer, or -1 if there is no
   * more data to be read. This method also fills the hdfs buffer with new data if it is empty.
   *
   * @return the next value in the stream from 0 to 255 or -1 if there is no more data to be read
   * @throws IOException if the bulk read from hdfs fails
   */
  private int readFromHdfsBuffer() throws IOException {
    if (mBufferPosition < mBufferLimit) {
      if (mStatistics != null) {
        mStatistics.incrementBytesRead(1);
      }
      mCurrentPosition++;
      return BufferUtils.byteToInt(mBuffer[mBufferPosition++]);
    }
    LOG.error("Reading from HDFS directly");
    while ((mBufferLimit = mHdfsInputStream.read(mBuffer)) == 0) {
      LOG.error("Read 0 bytes in readFromHdfsBuffer for {}", mHdfsPath);
    }
    if (mBufferLimit == -1) {
      return -1;
    }
    mBufferPosition = 0;
    if (mStatistics != null) {
      mStatistics.incrementBytesRead(1);
    }
    mCurrentPosition++;
    return BufferUtils.byteToInt(mBuffer[mBufferPosition++]);
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

    if (mAlluxioFileInputStream != null) {
      mAlluxioFileInputStream.seek(pos);
    } else {
      getHdfsInputStream(pos);
      // TODO(calvin): Optimize for the case when the data is still valid in the buffer
      // Invalidate buffer
      mBufferLimit = -1;
    }

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
}
