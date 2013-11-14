/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
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

import tachyon.CommonUtils;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemSingleLocal;

/**
 * <code>FileOutStream</code> implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by
 * the client code.
 */
public class FileOutStream extends OutStream {
  private final long BLOCK_CAPACITY;

  private BlockOutStream mCurrentBlockOutStream;
  private boolean mCanCache;
  private long mCurrentBlockId;
  private long mCurrentBlockLeftByte;
  private List<BlockOutStream> mPreviousBlockOutStreams;
  private long mWrittenBytes;

  private OutputStream mCheckpointOutputStream = null;
  private String mUnderFsFile = null;

  private boolean mClosed = false;
  private boolean mCancel = false;

  FileOutStream(TachyonFile file, WriteType opType) throws IOException {
    super(file, opType);

    BLOCK_CAPACITY = file.getBlockSizeByte();

    mCurrentBlockOutStream = null;
    mCanCache = true;
    mCurrentBlockId = -1;
    mCurrentBlockLeftByte = 0;
    mPreviousBlockOutStreams = new ArrayList<BlockOutStream>();
    mWrittenBytes = 0;

    if (WRITE_TYPE.isThrough()) {
      mUnderFsFile = TFS.createAndGetUserUnderfsTempFolder() + "/" + FILE.FID;
      UnderFileSystem underfsClient = UnderFileSystem.get(mUnderFsFile);
      if (BLOCK_CAPACITY > Integer.MAX_VALUE) {
        throw new IOException("BLOCK_CAPCAITY (" + BLOCK_CAPACITY + ") can not bigger than "
            + Integer.MAX_VALUE);
      }
      mCheckpointOutputStream = underfsClient.create(mUnderFsFile, (int) BLOCK_CAPACITY);
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
      mCurrentBlockId = TFS.getBlockIdBasedOnOffset(FILE.FID, mWrittenBytes);
      mCurrentBlockLeftByte = BLOCK_CAPACITY;

      mCurrentBlockOutStream =
          new BlockOutStream(FILE, WRITE_TYPE, (int) (mWrittenBytes / BLOCK_CAPACITY));
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (WRITE_TYPE.isCache() && mCanCache) {
      if (mCurrentBlockId == -1 || mCurrentBlockLeftByte == 0) {
        getNextBlock();
      }
      // TODO Cache the exception here.
      mCurrentBlockOutStream.write(b);
      mCurrentBlockLeftByte --;
    }

    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(b);
    }

    mWrittenBytes ++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }

    if (WRITE_TYPE.isCache()) {
      if (mCanCache) {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockLeftByte == 0) {
            getNextBlock();
          }
          if (mCurrentBlockLeftByte > tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCurrentBlockLeftByte -= tLen;
            mWrittenBytes += tLen;
            tOff += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) mCurrentBlockLeftByte);
            tOff += mCurrentBlockLeftByte;
            tLen -= mCurrentBlockLeftByte;
            mWrittenBytes += mCurrentBlockLeftByte;
            mCurrentBlockLeftByte = 0;
          }
        }
      } else if (WRITE_TYPE.isMustCache()) {
        throw new IOException("Can not cache: " + WRITE_TYPE);
      }
    }

    if (WRITE_TYPE.isThrough()) {
      mCheckpointOutputStream.write(b, off, len);
    }
  }

  public void write(ByteBuffer buf) throws IOException {
    write(buf.array(), buf.position(), buf.limit() - buf.position());
  }

  public void write(ArrayList<ByteBuffer> bufs) throws IOException {
    for (int k = 0; k < bufs.size(); k ++) {
      write(bufs.get(k));
    }
  }

  @Override
  public void flush() throws IOException {
    // We only flush the checkpoint output stream.
    // TODO flushing for RAMFS block streams.
    mCheckpointOutputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mCurrentBlockOutStream != null) {
        mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
      }

      if (mCancel) {
        if (WRITE_TYPE.isCache()) {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        }

        if (WRITE_TYPE.isThrough()) {
          mCheckpointOutputStream.close();
          UnderFileSystem underFsClient = UnderFileSystem.get(mUnderFsFile);
          underFsClient.delete(mUnderFsFile, false);
        }
      } else {
        if (WRITE_TYPE.isCache()) {
          try {
            for (int k = 0; k < mPreviousBlockOutStreams.size(); k ++) {
              mPreviousBlockOutStreams.get(k).close();
            }

            TFS.completeFile(FILE.FID);
          } catch (IOException e) {
            if (WRITE_TYPE == WriteType.MUST_CACHE) {
              throw e;
            }
          }
        }

        if (WRITE_TYPE.isThrough()) {
          mCheckpointOutputStream.flush();
          mCheckpointOutputStream.close();
          TFS.addCheckpoint(FILE.FID);
          TFS.completeFile(FILE.FID);
        }
      }
    }
    mClosed = true;
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
  }
}
