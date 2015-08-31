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

package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;

/**
 * <code>FileOutStream</code> implementation of TachyonFile. To get an instance of this class, one
 * should call the method <code>getOutStream</code> of <code>tachyon.client.TachyonFile</code>,
 * rather than constructing a new instance directly in the client code.
 */
public class FileOutStream extends OutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final long mBlockCapacityByte;

  private BlockOutStream mCurrentBlockOutStream;
  private List<BlockOutStream> mPreviousBlockOutStreams;
  private long mCachedBytes;

  private OutputStream mCheckpointOutputStream = null;
  private String mUnderFsFile = null;

  private boolean mClosed = false;
  private boolean mCancel = false;

  /**
   * @param file the output file
   * @param opType the OutStream's write type
   * @param ufsConf the under file system configuration
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @throws IOException if the underlying file cannot be found
   */
  FileOutStream(TachyonFile file, WriteType opType, Object ufsConf, TachyonConf tachyonConf)
      throws IOException {
    super(file, opType, tachyonConf);

    mBlockCapacityByte = file.getBlockSizeByte();

    // TODO(hy): Support and test append.
    mCurrentBlockOutStream = null;
    mPreviousBlockOutStreams = new ArrayList<BlockOutStream>();
    mCachedBytes = 0;

    if (mWriteType.isThrough()) {
      mUnderFsFile = PathUtils.concatPath(mTachyonFS.createAndGetUserUfsTempFolder(ufsConf),
          mFile.mFileId);
      UnderFileSystem underfsClient = UnderFileSystem.get(mUnderFsFile, ufsConf, tachyonConf);
      if (mBlockCapacityByte > Integer.MAX_VALUE) {
        throw new IOException("BLOCK_CAPACITY (" + mBlockCapacityByte + ") can not bigger than "
            + Integer.MAX_VALUE);
      }
      mCheckpointOutputStream = underfsClient.create(mUnderFsFile, (int) mBlockCapacityByte);
    }
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCurrentBlockOutStream != null) {
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    Boolean canComplete = false;
    if (mWriteType.isThrough()) {
      if (mCancel) {
        mCheckpointOutputStream.close();
        UnderFileSystem underFsClient = UnderFileSystem.get(mUnderFsFile, mTachyonConf);
        underFsClient.delete(mUnderFsFile, false);
      } else {
        mCheckpointOutputStream.flush();
        mCheckpointOutputStream.close();
        mTachyonFS.addCheckpoint(mFile.mFileId);
        canComplete = true;
      }
    }

    if (mWriteType.isCache()) {
      try {
        if (mCancel) {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.close();
          }
          canComplete = true;
        }
      } catch (IOException ioe) {
        if (mWriteType.isMustCache()) {
          LOG.error(ioe.getMessage(), ioe);
          throw new IOException("Fail to cache: " + mWriteType + ", message: " + ioe.getMessage(),
              ioe);
        } else {
          LOG.warn("Fail to cache for: ", ioe);
        }
      }
    }

    if (canComplete) {
      if (mWriteType.isAsync()) {
        mTachyonFS.asyncCheckpoint(mFile.mFileId);
      }
      mTachyonFS.completeFile(mFile.mFileId);
    }
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    // TODO(hy):  We only flush the checkpoint output stream. Flush for RAMFS block streams.
    if (mWriteType.isThrough()) {
      mCheckpointOutputStream.flush();
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      if (mCurrentBlockOutStream.getRemainingSpaceBytes() > 0) {
        throw new IOException("The current block still has space left, no need to get new block");
      }
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mWriteType.isCache()) {
      int offset = (int) (mCachedBytes / mBlockCapacityByte);
      mCurrentBlockOutStream = BlockOutStream.get(mFile, mWriteType, offset, mTachyonConf);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }

    if (mWriteType.isCache()) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockOutStream == null
              || mCurrentBlockOutStream.getRemainingSpaceBytes() == 0) {
            getNextBlock();
          }
          long currentBlockLeftBytes = mCurrentBlockOutStream.getRemainingSpaceBytes();
          if (currentBlockLeftBytes >= tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCachedBytes += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
            mCachedBytes += currentBlockLeftBytes;
          }
        }
      } catch (IOException e) {
        if (mWriteType.isMustCache()) {
          LOG.error(e.getMessage(), e);
          throw new IOException("Fail to cache: " + mWriteType + ", message: " + e.getMessage(), e);
        } else {
          LOG.warn("Fail to cache for: ", e);
        }
      }
    }

    if (mWriteType.isThrough()) {
      mCheckpointOutputStream.write(b, off, len);
      mTachyonFS.getClientMetrics().incBytesWrittenUfs(len);
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (mWriteType.isCache()) {
      try {
        if (mCurrentBlockOutStream == null
            || mCurrentBlockOutStream.getRemainingSpaceBytes() == 0) {
          getNextBlock();
        }
        // TODO(hy): Cache the exception here.
        mCurrentBlockOutStream.write(b);
        mCachedBytes ++;
      } catch (IOException e) {
        if (mWriteType.isMustCache()) {
          LOG.error(e.getMessage(), e);
          throw new IOException("Fail to cache: " + mWriteType + ", message: " + e.getMessage(), e);
        } else {
          LOG.warn("Fail to cache for: ", e);
        }
      }
    }

    if (mWriteType.isThrough()) {
      mCheckpointOutputStream.write(b);
      mTachyonFS.getClientMetrics().incBytesWrittenUfs(1);
    }
  }
}
