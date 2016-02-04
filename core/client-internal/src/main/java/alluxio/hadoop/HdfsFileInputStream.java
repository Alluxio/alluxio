/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

import alluxio.Constants;
import alluxio.TachyonURI;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.conf.TachyonConf;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.TachyonException;
import alluxio.util.io.BufferUtils;

/**
 * An input stream for reading a file from HDFS.
 */
@NotThreadSafe
public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private long mCurrentPosition;
  private Path mHdfsPath;
  private Configuration mHadoopConf;
  private int mHadoopBufferSize;
  private Statistics mStatistics;
  private URIStatus mFileInfo;

  private FSDataInputStream mHdfsInputStream = null;

  private FileInStream mTachyonFileInputStream = null;

  private boolean mClosed = false;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte[] mBuffer;

  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param uri the Tachyon file URI
   * @param hdfsPath the HDFS path
   * @param conf Hadoop configuration
   * @param bufferSize the buffer size
   * @param stats filesystem statistics
   * @throws IOException if the underlying file does not exist or its stream cannot be created
   */
  public HdfsFileInputStream(TachyonURI uri, Path hdfsPath, Configuration conf, int bufferSize,
      org.apache.hadoop.fs.FileSystem.Statistics stats) throws IOException {
    LOG.debug("HdfsFileInputStream({}, {}, {}, {}, {})", uri, hdfsPath, conf,
        bufferSize, stats);
    TachyonConf tachyonConf = ClientContext.getConf();
    long bufferBytes = tachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES);
    mBuffer = new byte[Ints.checkedCast(bufferBytes) * 4];
    mCurrentPosition = 0;
    FileSystem fs = FileSystem.Factory.get();
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;
    mStatistics = stats;
    try {
      mFileInfo = fs.getStatus(uri);
      mTachyonFileInputStream =
          fs.openFile(uri, OpenFileOptions.defaults().setReadType(ReadType.CACHE));
    } catch (FileDoesNotExistException e) {
      throw new FileNotFoundException(
          ExceptionMessage.HDFS_FILE_NOT_FOUND.getMessage(hdfsPath, uri));
    } catch (TachyonException e) {
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
    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.close();
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
    if (mTachyonFileInputStream != null) {
      int ret = 0;
      try {
        ret = mTachyonFileInputStream.read();
        if (mStatistics != null && ret != -1) {
          mStatistics.incrementBytesRead(1);
        }
        mCurrentPosition ++;
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mTachyonFileInputStream.close();
        mTachyonFileInputStream = null;
      }
    }
    getHdfsInputStream();
    return readFromHdfsBuffer();
  }

  @Override
  public int read(byte[] b) throws IOException {
    throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (mClosed) {
      throw new IOException("Cannot read from a closed stream.");
    }
    if (mTachyonFileInputStream != null) {
      int ret = 0;
      try {
        ret = mTachyonFileInputStream.read(b, off, len);
        if (mStatistics != null && ret != -1) {
          mStatistics.incrementBytesRead(ret);
        }
        mCurrentPosition += ret;
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mTachyonFileInputStream.close();
        mTachyonFileInputStream = null;
      }
    }

    getHdfsInputStream();
    int byteRead = readFromHdfsBuffer();
    // byteRead is an unsigned byte, if its -1 then we have hit EOF
    if (byteRead == -1) {
      return -1;
    }
    // Convert byteRead back to a signed byte
    b[off] = (byte) byteRead;
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

    if (mTachyonFileInputStream != null) {
      try {
        mTachyonFileInputStream.seek(position);
        ret = mTachyonFileInputStream.read(buffer, offset, length);
        if (mStatistics != null && ret != -1) {
          mStatistics.incrementBytesRead(ret);
        }
        return ret;
      } finally {
        mTachyonFileInputStream.seek(oldPos);
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
      mCurrentPosition ++;
      return BufferUtils.byteToInt(mBuffer[mBufferPosition ++]);
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
    mCurrentPosition ++;
    return BufferUtils.byteToInt(mBuffer[mBufferPosition ++]);
  }

  /**
   * This method is not supported in {@link HdfsFileInputStream}.
   *
   * @param position N/A
   * @param buffer N/A
   * @throws IOException always
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
  }

  /**
   * This method is not supported in {@link HdfsFileInputStream}.
   *
   * @param position N/A
   * @param buffer N/A
   * @param offset N/A
   * @param length N/A
   * @throws IOException always
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
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

    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.seek(pos);
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
