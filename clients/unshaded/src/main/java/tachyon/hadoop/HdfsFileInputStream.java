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

package tachyon.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

import tachyon.Constants;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.file.FileInStream;
import tachyon.conf.TachyonConf;

public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private long mCurrentPosition;
  private TachyonFS mTFS;
  private long mFileId;
  private Path mHdfsPath;
  private Configuration mHadoopConf;
  private int mHadoopBufferSize;
  private Statistics mStatistics;
  private TachyonFile mTachyonFile;

  private FSDataInputStream mHdfsInputStream = null;

  private FileInStream mTachyonFileInputStream = null;

  private boolean mClosed = false;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte[] mBuffer;

  private final TachyonConf mTachyonConf;

  /**
   * @param tfs the TachyonFS
   * @param fileId the file id
   * @param hdfsPath the HDFS path
   * @param conf Hadoop configuration
   * @param bufferSize the buffer size
   * @param stats filesystem statistics
   * @param tachyonConf Tachyon configuration
   * @throws IOException if the underlying file does not exist or its stream cannot be created
   */
  public HdfsFileInputStream(TachyonFS tfs, long fileId, Path hdfsPath, Configuration conf,
      int bufferSize, FileSystem.Statistics stats, TachyonConf tachyonConf) throws IOException {
    LOG.debug("HdfsFileInputStream({}, {}, {}, {}, {}, {})", tfs, fileId, hdfsPath, conf,
        bufferSize, stats);
    mTachyonConf = tachyonConf;
    long bufferBytes = mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES);
    mBuffer = new byte[Ints.checkedCast(bufferBytes) * 4];
    mCurrentPosition = 0;
    mTFS = tfs;
    mFileId = fileId;
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;
    mStatistics = stats;
    mTachyonFile = mTFS.getFile(mFileId);
    if (mTachyonFile == null) {
      throw new FileNotFoundException("File " + hdfsPath + " with FID " + fileId
          + " is not found.");
    }
    mTachyonFile.setUFSConf(mHadoopConf);
    mTachyonFileInputStream = mTachyonFile.getInStream(ReadType.CACHE);
  }

  /**
   * This method is not supported in <code>HdfsFileInputStream</code>.
   *
   * @return N/A
   * @throws IOException always
   */
  @Override
  public int available() throws IOException {
    throw new IOException("Not supported");
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

  private void getHdfsInputStream() throws IOException {
    if (mHdfsInputStream == null) {
      FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
      mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
      mHdfsInputStream.seek(mCurrentPosition);
    }
  }

  private void getHdfsInputStream(long position) throws IOException {
    if (mHdfsInputStream == null) {
      FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
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
        mTachyonFileInputStream = null;
      }
    }
    getHdfsInputStream();
    return readFromHdfsBuffer();
  }

  @Override
  public int read(byte[] b) throws IOException {
    throw new IOException("Not supported");
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
        mTachyonFileInputStream = null;
      }
    }

    getHdfsInputStream();
    b[off] = (byte) readFromHdfsBuffer();
    if (b[off] == -1) {
      return -1;
    }
    return 1;
  }

  @Override
  public synchronized int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if (mClosed) {
      throw new IOException("Cannot read from a closed stream.");
    }
    int ret = -1;
    long oldPos = getPos();
    if ((position < 0) || (position >= mTachyonFile.length())) {
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

  private int readFromHdfsBuffer() throws IOException {
    if (mBufferPosition < mBufferLimit) {
      if (mStatistics != null) {
        mStatistics.incrementBytesRead(1);
      }
      return mBuffer[mBufferPosition ++];
    }
    LOG.error("Reading from HDFS directly");
    while ((mBufferLimit = mHdfsInputStream.read(mBuffer)) == 0) {
      LOG.error("Read 0 bytes in readFromHdfsBuffer for " + mHdfsPath);
    }
    if (mBufferLimit == -1) {
      return -1;
    }
    mBufferPosition = 0;
    if (mStatistics != null) {
      mStatistics.incrementBytesRead(1);
    }
    return mBuffer[mBufferPosition ++];
  }

  /**
   * This method is not supported in <code>HdfsFileInputStream</code>.
   *
   * @throws IOException always
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * This method is not supported in <code>HdfsFileInputStream</code>.
   *
   * @throws IOException always
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos == mCurrentPosition) {
      return;
    }

    if (pos < 0) {
      throw new IOException("Seek position is negative: " + pos);
    }
    if (pos > mTachyonFile.length()) {
      throw new IOException("Seek position is past EOF: " + pos + ", fileSize = "
          + mTachyonFile.length());
    }

    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.seek(pos);
    } else {
      getHdfsInputStream(pos);
    }

    mCurrentPosition = pos;
  }

  /**
   * This method is not supported in <code>HdfsFileInputStream</code>.
   *
   * @return N/A
   * @throws IOException always
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Not supported");
  }
}
