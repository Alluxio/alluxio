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
package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * <code>FileOutStream</code> implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 */
public class FileOutStream extends OutStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final long BLOCK_CAPACITY;

  private BlockOutStream mCurrentBlockOutStream;
  private long mCurrentBlockId;
  private long mCurrentBlockLeftByte;
  private List<BlockOutStream> mPreviousBlockOutStreams;
  private long mCachedBytes;

  private OutputStream mCheckpointOutputStream = null;
  private String mUnderFsFile = null;

  private boolean mClosed = false;
  private boolean mCancel = false;

  /**
   * @param file
   *            the output file
   * @param opType
   *            the OutStream's write type
   * @param ufsConf
   *            the under file system configuration
   * @throws IOException
   */
  FileOutStream(TachyonFile file, WriteType opType, Object ufsConf) throws IOException {
    super(file, opType);

    BLOCK_CAPACITY = file.getBlockSizeByte();

    // TODO Support and test append.
    mCurrentBlockOutStream = null;
    mCurrentBlockId = -1;
    mCurrentBlockLeftByte = 0;
    mPreviousBlockOutStreams = new ArrayList<BlockOutStream>();
    mCachedBytes = 0;

    if (WRITE_TYPE.isThrough()) {
      mUnderFsFile = CommonUtils.concat(TFS.createAndGetUserUnderfsTempFolder(), FILE.FID);
      UnderFileSystem underfsClient = UnderFileSystem.get(mUnderFsFile, ufsConf);
      if (BLOCK_CAPACITY > Integer.MAX_VALUE) {
        throw new IOException("BLOCK_CAPCAITY (" + BLOCK_CAPACITY + ") can not bigger than "
            + Integer.MAX_VALUE);
      }
      mCheckpointOutputStream = underfsClient.create(mUnderFsFile, (int) BLOCK_CAPACITY);
    }
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mCurrentBlockOutStream != null) {
        mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
      }

      Boolean canComplete = false;
      if (WRITE_TYPE.isThrough()) {
        if (mCancel) {
          mCheckpointOutputStream.close();
          UnderFileSystem underFsClient = UnderFileSystem.get(mUnderFsFile);
          underFsClient.delete(mUnderFsFile, false);
        } else {
          mCheckpointOutputStream.flush();
          mCheckpointOutputStream.close();
          TFS.addCheckpoint(FILE.FID);
          canComplete = true;
        }
      }

      if (WRITE_TYPE.isCache()) {
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
          if (WRITE_TYPE.isMustCache()) {
            LOG.error(ioe.getMessage());
            throw new IOException("Fail to cache: " + WRITE_TYPE);
          } else {
            LOG.warn("Fail to cache for: " + ioe.getMessage());
          }
        }
      }

      if (canComplete) {
        if (WRITE_TYPE.isAsync()) {
          TFS.asyncCheckpoint(FILE.FID);
        }
        TFS.completeFile(FILE.FID);
      }
    }

    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    // TODO We only flush the checkpoint output stream. Flush for RAMFS block streams.
    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.flush();
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockId != -1) {
      if (mCurrentBlockLeftByte != 0) {
        throw new IOException("The current block still has space left, no need to get new block");
      }
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (WRITE_TYPE.isCache()) {
      mCurrentBlockId = TFS.getBlockIdBasedOnOffset(FILE.FID, mCachedBytes);
      mCurrentBlockLeftByte = BLOCK_CAPACITY;

      mCurrentBlockOutStream =
          new BlockOutStream(FILE, WRITE_TYPE, (int) (mCachedBytes / BLOCK_CAPACITY));
    }
  }

  /**
   * Write all the bufs in the list one by one
   *
   * @param bufs
   *            the bufs
   * @throws IOException
   */
  public void write(ArrayList<ByteBuffer> bufs) throws IOException {
    for (int k = 0; k < bufs.size(); k ++) {
      write(bufs.get(k));
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

    if (WRITE_TYPE.isCache()) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockLeftByte == 0) {
            getNextBlock();
          } else if (mCurrentBlockLeftByte < 0 || mCurrentBlockOutStream == null) {
            throw new IOException("mCurrentBlockLeftByte " + mCurrentBlockLeftByte + " "
                + mCurrentBlockOutStream);
          }
          if (mCurrentBlockLeftByte >= tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCurrentBlockLeftByte -= tLen;
            mCachedBytes += tLen;
            tOff += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) mCurrentBlockLeftByte);
            tOff += mCurrentBlockLeftByte;
            tLen -= mCurrentBlockLeftByte;
            mCachedBytes += mCurrentBlockLeftByte;
            mCurrentBlockLeftByte = 0;
          }
        }
      } catch (IOException ioe) {
        if (WRITE_TYPE.isMustCache()) {
          LOG.error(ioe.getMessage());
          throw new IOException("Fail to cache: " + WRITE_TYPE);
        } else {
          LOG.warn("Fail to cache for: " + ioe.getMessage());
        }
      }
    }

    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(b, off, len);
    }
  }

  /**
   * Write a ByteBuffer to the OutStream
   *
   * @param buf
   *            the ByteBuffer to be written
   * @throws IOException
   */
  public void write(ByteBuffer buf) throws IOException {
    write(buf.array(), buf.position(), buf.limit() - buf.position());
  }

  @Override
  public void write(int b) throws IOException {
    if (WRITE_TYPE.isCache()) {
      try {
        if (mCurrentBlockId == -1 || mCurrentBlockLeftByte == 0) {
          getNextBlock();
        }
        // TODO Cache the exception here.
        mCurrentBlockOutStream.write(b);
        mCurrentBlockLeftByte --;
        mCachedBytes ++;
      } catch (IOException ioe) {
        if (WRITE_TYPE.isMustCache()) {
          LOG.error(ioe.getMessage());
          throw new IOException("Fail to cache: " + WRITE_TYPE);
        } else {
          LOG.warn("Fail to cache for: " + ioe.getMessage());
        }
      }
    }

    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(b);
    }
  }
}
