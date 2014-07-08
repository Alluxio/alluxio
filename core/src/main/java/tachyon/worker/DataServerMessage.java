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
package tachyon.worker;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.client.TachyonByteBuffer;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

/**
 * The message type used to send data request and response for remote data.
 */
public class DataServerMessage {
  public static final short DATA_SERVER_REQUEST_MESSAGE = 1;
  public static final short DATA_SERVER_RESPONSE_MESSAGE = 2;

  public static DataServerMessage createBlockRequestMessage() {
    DataServerMessage ret = new DataServerMessage(false, DATA_SERVER_REQUEST_MESSAGE);
    ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
    return ret;
  }

  public static DataServerMessage createBlockRequestMessage(long blockId) {
    return createBlockRequestMessage(blockId, 0, -1);
  }

  public static DataServerMessage createBlockRequestMessage(long blockId, long offset, long len) {
    DataServerMessage ret = new DataServerMessage(true, DATA_SERVER_REQUEST_MESSAGE);

    ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
    ret.mBlockId = blockId;
    ret.mOffset = offset;
    ret.mLength = len;
    ret.generateHeader();
    ret.mData = ByteBuffer.allocate(0);
    ret.mIsMessageReady = true;

    return ret;
  }

  public static DataServerMessage createBlockResponseMessage(boolean toSend, long blockId) {
    return createBlockResponseMessage(toSend, blockId, 0, -1);
  }

  public static DataServerMessage createBlockResponseMessage(boolean toSend, long blockId,
      long offset, long len) {
    DataServerMessage ret = new DataServerMessage(toSend, DATA_SERVER_RESPONSE_MESSAGE);

    if (toSend) {
      ret.mBlockId = blockId;

      try {
        if (offset < 0) {
          throw new IOException("Offset can not be negative: " + offset);
        }
        if (len < 0 && len != -1) {
          throw new IOException("Length can not be negative except -1: " + len);
        }

        String filePath = CommonUtils.concat(WorkerConf.get().DATA_FOLDER, blockId);
        ret.LOG.info("Try to response remote requst by reading from " + filePath);
        RandomAccessFile file = new RandomAccessFile(filePath, "r");

        long fileLength = file.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error =
              String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
                  len, fileLength);
        }
        if (error != null) {
          file.close();
          throw new IOException(error);
        }

        if (len == -1) {
          len = fileLength - offset;
        }

        ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
        ret.mOffset = offset;
        ret.mLength = len;
        FileChannel channel = file.getChannel();
        ret.mTachyonData = null;
        ret.mData = channel.map(FileChannel.MapMode.READ_ONLY, offset, len);
        channel.close();
        file.close();
        ret.mIsMessageReady = true;
        ret.generateHeader();
        ret.LOG.info("Response remote requst by reading from " + filePath + " preparation done.");
      } catch (Exception e) {
        // TODO This is a trick for now. The data may have been removed before remote retrieving.
        ret.mBlockId = -ret.mBlockId;
        ret.mLength = 0;
        ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
        ret.mData = ByteBuffer.allocate(0);
        ret.mIsMessageReady = true;
        ret.generateHeader();
        ret.LOG.error("The file is not here : " + e.getMessage(), e);
      }
    } else {
      ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
      ret.mData = null;
    }

    return ret;
  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final boolean IS_TO_SEND_DATA;
  private final short mMsgType;
  private boolean mIsMessageReady;
  private ByteBuffer mHeader;

  private static final int HEADER_LENGTH = 26;
  private long mBlockId;

  private long mOffset;

  private long mLength;

  private int mLockId = -1;

  private TachyonByteBuffer mTachyonData = null;

  private ByteBuffer mData = null;

  private DataServerMessage(boolean isToSendData, short msgType) {
    IS_TO_SEND_DATA = isToSendData;
    mMsgType = msgType;
    mIsMessageReady = false;
  }

  public void checkReady() {
    if (!mIsMessageReady) {
      CommonUtils.runtimeException("Message is not ready.");
    }
  }

  public void close() {
    if (mMsgType == DATA_SERVER_RESPONSE_MESSAGE) {
      try {
        if (mTachyonData != null) {
          mTachyonData.close();
        }
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
  }

  public boolean finishSending() {
    isSend(true);

    return mHeader.remaining() == 0 && mData.remaining() == 0;
  }

  private void generateHeader() {
    mHeader.clear();
    mHeader.putShort(mMsgType);
    mHeader.putLong(mBlockId);
    mHeader.putLong(mOffset);
    mHeader.putLong(mLength);
    mHeader.flip();
  }

  public long getBlockId() {
    checkReady();
    return mBlockId;
  }

  public long getLength() {
    checkReady();
    return mLength;
  }

  int getLockId() {
    return mLockId;
  }

  public long getOffset() {
    checkReady();
    return mOffset;
  }

  public ByteBuffer getReadOnlyData() {
    if (!mIsMessageReady) {
      CommonUtils.runtimeException("Message is not ready.");
    }
    ByteBuffer ret = mData.asReadOnlyBuffer();
    ret.flip();
    return ret;
  }

  public boolean isMessageReady() {
    return mIsMessageReady;
  }

  private void isSend(boolean isSend) {
    if (IS_TO_SEND_DATA != isSend) {
      if (IS_TO_SEND_DATA) {
        CommonUtils.runtimeException("Try to recv on send message");
      } else {
        CommonUtils.runtimeException("Try to send on recv message");
      }
    }
  }

  public int recv(SocketChannel socketChannel) throws IOException {
    isSend(false);

    int numRead = 0;
    if (mHeader.remaining() > 0) {
      numRead = socketChannel.read(mHeader);
      if (mHeader.remaining() == 0) {
        mHeader.flip();
        short msgType = mHeader.getShort();
        assert (mMsgType == msgType);
        mBlockId = mHeader.getLong();
        mOffset = mHeader.getLong();
        mLength = mHeader.getLong();
        // TODO make this better to truncate the file.
        assert mLength < Integer.MAX_VALUE;
        if (mMsgType == DATA_SERVER_RESPONSE_MESSAGE) {
          if (mLength == -1) {
            mData = ByteBuffer.allocate(0);
          } else {
            mData = ByteBuffer.allocate((int) mLength);
          }
        }
        LOG.info(String.format("data" + mData + ", blockId(%d), offset(%d), dataLength(%d)",
            mBlockId, mOffset, mLength));
        if (mMsgType == DATA_SERVER_REQUEST_MESSAGE || mLength <= 0) {
          mIsMessageReady = true;
        }
      }
    } else {
      numRead = socketChannel.read(mData);
      if (mData.remaining() == 0) {
        mIsMessageReady = true;
      }
    }

    return numRead;
  }

  public void send(SocketChannel socketChannel) throws IOException {
    isSend(true);

    socketChannel.write(mHeader);

    if (mHeader.remaining() == 0) {
      socketChannel.write(mData);
    }
  }

  void setLockId(int lockId) {
    mLockId = lockId;
  }
}
