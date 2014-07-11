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

  /**
   * Create a default block request message, just allocate the message header, and no attribute is
   * set. The message is not ready to be sent.
   * 
   * @return the created block request message
   */
  public static DataServerMessage createBlockRequestMessage() {
    DataServerMessage ret = new DataServerMessage(false, DATA_SERVER_REQUEST_MESSAGE);
    ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
    return ret;
  }

  /**
   * Create a block request message specified by the block's id, and the message is ready to be
   * sent.
   * 
   * @param blockId
   *          The id of the block
   * @return The created block request message
   */
  public static DataServerMessage createBlockRequestMessage(long blockId) {
    return createBlockRequestMessage(blockId, 0, -1);
  }

  /**
   * Create a block request message specified by the block's id, the offset and the length. The
   * message is ready to be sent. If <code>len</code> is -1, it means request the data from offset
   * to the block's end.
   * 
   * @param blockId
   *          The id of the block
   * @param offset
   *          The requested data's offset in the block
   * @param len
   *          The length of the requested data. If it's -1, it means request the data from offset to
   *          the block's end.
   * @return The created block request message
   */
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

  /**
   * Create a block response message specified by the block's id. If <code>toSend</code> is true, it
   * will prepare the data to be sent, otherwise the message is used to receive data.
   * 
   * @param toSend
   *          If true the message is to send the data, otherwise it's used to receive data.
   * @param blockId
   *          The id of the block
   * @return The created block response message
   */
  public static DataServerMessage createBlockResponseMessage(boolean toSend, long blockId) {
    return createBlockResponseMessage(toSend, blockId, 0, -1);
  }

  /**
   * Create a block response message specified by the block's id, the offset and the length. If
   * <code>toSend</code> is true, it will prepare the data to be sent, otherwise the message is used
   * to receive data. If <code>len</code> is -1, it means response the data from offset to the
   * block's end.
   * 
   * @param toSend
   *          If true the message is to send the data, otherwise it's used to receive data
   * @param blockId
   *          The id of the block
   * @param offset
   *          The responded data's offset in the block
   * @param len
   *          The length of the responded data. If it's -1, it means respond the data from offset to
   *          the block's end.
   * @return The created block response message
   */
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

  /**
   * New a DataServerMessage. Notice that it's not ready.
   * 
   * @param isToSendData
   *          true if this is a send message, otherwise this is a recv message
   * @param msgType
   *          The message type
   */
  private DataServerMessage(boolean isToSendData, short msgType) {
    IS_TO_SEND_DATA = isToSendData;
    mMsgType = msgType;
    mIsMessageReady = false;
  }

  /**
   * Check if the message is ready. If not ready, it will throw a runtime exception.
   */
  public void checkReady() {
    if (!mIsMessageReady) {
      CommonUtils.runtimeException("Message is not ready.");
    }
  }

  /**
   * Close the message.
   */
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

  /**
   * Return whether the message finishes sending or not. It will check if the message is a send
   * message, so don't call this on a recv message.
   * 
   * @return true if the message finishes sending, false otherwise
   */
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

  /**
   * Get the id of the block. Make sure the message is ready before calling this method.
   * 
   * @return The id of the block
   */
  public long getBlockId() {
    checkReady();
    return mBlockId;
  }

  /**
   * Get the length of the message's requested or responded data. Make sure the message is ready
   * before calling this method.
   * 
   * @return The length of the message's requested or responded data
   */
  public long getLength() {
    checkReady();
    return mLength;
  }

  /**
   * Get the id of the block's locker.
   * 
   * @return The id of the block's locker
   */
  int getLockId() {
    return mLockId;
  }

  /**
   * Get the offset of the message's data in the block. Make sure the message is ready before
   * calling this method.
   * 
   * @return The offset of the message's data in the block
   */
  public long getOffset() {
    checkReady();
    return mOffset;
  }

  /**
   * Get the read only buffer of the message's data. Make sure the message is ready before calling
   * this method.
   * 
   * @return The read only buffer of the message's data
   */
  public ByteBuffer getReadOnlyData() {
    checkReady();
    ByteBuffer ret = mData.asReadOnlyBuffer();
    ret.flip();
    return ret;
  }

  /**
   * @return true if the message is ready, false otherwise
   */
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

  /**
   * Use this message to receive from the specified socket channel. Make sure this is a recv
   * message and the message type is matched.
   * 
   * @param socketChannel
   *          The socket channel to receive from
   * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
   * @throws IOException
   */
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

  /**
   * Send this message to the specified socket channel. Make sure this is a send message.
   * 
   * @param socketChannel
   *          The socket channel to send to
   * @throws IOException
   */
  public void send(SocketChannel socketChannel) throws IOException {
    isSend(true);

    socketChannel.write(mHeader);

    if (mHeader.remaining() == 0) {
      socketChannel.write(mData);
    }
  }

  /**
   * Set the id of the block's locker.
   * 
   * @param lockId
   *          The id of the block's locker
   */
  void setLockId(int lockId) {
    mLockId = lockId;
  }
}
