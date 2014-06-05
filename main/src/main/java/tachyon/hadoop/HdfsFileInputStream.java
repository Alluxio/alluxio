/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.conf.UserConf;

public class HdfsFileInputStream extends InputStream implements Seekable, PositionedReadable {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private long mCurrentPosition;
  private TachyonFS mTFS;
  private int mFileId;
  private Path mHdfsPath;
  private Configuration mHadoopConf;
  private int mHadoopBufferSize;

  private FSDataInputStream mHdfsInputStream = null;

  private InStream mTachyonFileInputStream = null;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte mBuffer[] = new byte[UserConf.get().FILE_BUFFER_BYTES * 4];

  public HdfsFileInputStream(TachyonFS tfs, int fileId, Path hdfsPath, Configuration conf,
      int bufferSize) throws IOException {
    LOG.debug("PartitionInputStreamHdfs(" + tfs + ", " + fileId + ", " + hdfsPath + ", " + conf
        + ", " + bufferSize + ")");
    mCurrentPosition = 0;
    mTFS = tfs;
    mFileId = fileId;
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;

    TachyonFile tachyonFile = mTFS.getFile(mFileId);
    if (tachyonFile == null) {
      throw new FileNotFoundException("File " + hdfsPath + " with FID " + fileId
          + " is not found.");
    }
    tachyonFile.setUFSConf(mHadoopConf);
    try {
      mTachyonFileInputStream = tachyonFile.getInStream(ReadType.CACHE);
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

    if (mHdfsInputStream != null) {
      return readFromHdfsBuffer();
    }

    FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
    mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
    mHdfsInputStream.seek(mCurrentPosition);

    return readFromHdfsBuffer();
  }

  @Override
  public int read(byte b[]) throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
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

    if (mHdfsInputStream != null) {
      b[off] = (byte) readFromHdfsBuffer();
      if (b[off] == -1) {
        return -1;
      }
      return 1;
    }

    FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
    mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
    mHdfsInputStream.seek(mCurrentPosition);

    b[off] = (byte) readFromHdfsBuffer();
    if (b[off] == -1) {
      return -1;
    }
    return 1;
  }

  /**
   * Read upto the specified number of bytes, from a given position within a
   * file, and return the number of bytes read. This does not change the current
   * offset of a file, and is thread-safe.
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new IOException("Not supported");
    // TODO Auto-generated method stub
    // return 0;
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
   * Read number of bytes equalt to the length of the buffer, from a given
   * position within a file. This does not change the current offset of a file,
   * and is thread-safe.
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    // TODO Auto-generated method stub
    throw new IOException("Not supported");
  }

  /**
   * Read the specified number of bytes, from a given position within a file.
   * This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    // TODO Auto-generated method stub
    throw new IOException("Not supported");
  }

  /**
   * Seek to the given offset from the start of the file. The next read() will
   * be from that location. Can't seek past the end of the file.
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos == mCurrentPosition) {
      return;
    }
    if (mTachyonFileInputStream != null) {
      mTachyonFileInputStream.seek(pos);
    } else if (mHdfsInputStream != null) {
      mHdfsInputStream.seek(pos);
    } else {
      FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
      mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
      mHdfsInputStream.seek(pos);
    }

    mCurrentPosition = pos;
  }

  /**
   * Seeks a different copy of the data. Returns true if found a new source,
   * false otherwise.
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Not supported");
    // TODO Auto-generated method stub
    // return false;
  }
}