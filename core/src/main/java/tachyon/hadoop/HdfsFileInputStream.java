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
import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.conf.UserConf;

public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private long mCurrentPosition;
  private TachyonFS mTFS;
  private int mFileId;
  private Path mHdfsPath;
  private Configuration mHadoopConf;
  private int mHadoopBufferSize;
  private TachyonFile mTachyonFile;

  private FSDataInputStream mHdfsInputStream = null;

  private InStream mTachyonFileInputStream = null;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte[] mBuffer = new byte[UserConf.get().FILE_BUFFER_BYTES * 4];

  public HdfsFileInputStream(TachyonFS tfs, int fileId, Path hdfsPath, Configuration conf,
      int bufferSize) throws IOException {
    LOG.debug("PartitionInputStreamHdfs({}, {}, {}, {}, {})", tfs, fileId, hdfsPath, conf,
        bufferSize);
    mCurrentPosition = 0;
    mTFS = tfs;
    mFileId = fileId;
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;
    mTachyonFile = mTFS.getFile(mFileId);
    if (mTachyonFile == null) {
      throw new FileNotFoundException("File " + hdfsPath + " with FID " + fileId
          + " is not found.");
    }
    mTachyonFile.setUFSConf(mHadoopConf);
    try {
      mTachyonFileInputStream = mTachyonFile.getInStream(ReadType.CACHE);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.close();
    }
    if (mHdfsInputStream != null) {
      mHdfsInputStream.close();
    }
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
    mHdfsInputStream.seek(position);;
  }

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public long getPos() throws IOException {
    return mCurrentPosition;
  }

  @Override
  public int read() throws IOException {
    if (mTachyonFileInputStream != null) {
      int ret = 0;
      try {
        ret = mTachyonFileInputStream.read();
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
    if (mTachyonFileInputStream != null) {
      int ret = 0;
      try {
        ret = mTachyonFileInputStream.read(b, off, len);
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

  /**
   * Read upto the specified number of bytes, from a given position within a file, and return the
   * number of bytes read. This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public synchronized int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    int ret = -1;
    long oldPos = getPos();
    if ((position < 0) || (position >= mTachyonFile.length())) {
      return ret;
    }

    if (mTachyonFileInputStream != null) {
      try {
        mTachyonFileInputStream.seek(position);
        ret = mTachyonFileInputStream.read(buffer, offset, length);
        return ret;
      } finally {
        mTachyonFileInputStream.seek(oldPos);
      }
    }

    try {
      getHdfsInputStream(position);
      ret = mHdfsInputStream.read(buffer, offset, length);
      return ret;
    } finally {
      if (mHdfsInputStream != null) {
        mHdfsInputStream.seek(oldPos);
      }
    }
  }

  private int readFromHdfsBuffer() throws IOException {
    if (mBufferPosition < mBufferLimit) {
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
    return mBuffer[mBufferPosition ++];
  }

  /**
   * Read number of bytes equalt to the length of the buffer, from a given position within a file.
   * This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Read the specified number of bytes, from a given position within a file. This does not change
   * the current offset of a file, and is thread-safe.
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * Seek to the given offset from the start of the file. The next read() will be from that
   * location. Can't seek past the end of the file.
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos == mCurrentPosition) {
      return;
    }

    if (pos < 0) {
      throw new IllegalArgumentException("Seek position is negative: " + pos);
    } else if (pos > mTachyonFile.length()) {
      throw new IllegalArgumentException("Seek position is past EOF: " + pos + ", fileSize = "
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
   * Seeks a different copy of the data. Returns true if found a new source, false otherwise.
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Not supported");
  }
}
