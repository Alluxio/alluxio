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

package tachyon.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.network.protocol.RPCMessage;

/**
 * The message type used to send data request and response for remote data.
 */
public class DataServerMessage {
  public static final short DATA_SERVER_REQUEST_MESSAGE = 1;
  public static final short DATA_SERVER_RESPONSE_MESSAGE = 2;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // TODO: convert client to netty to better deal with headers.
  private static final int HEADER_LENGTH = 38;

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
   * Create a block request message for an entire block by the block id, and the message is ready to
   * be sent.
   *
   * @param blockId The id of the block
   * @return The created block request message
   */
  public static DataServerMessage createBlockRequestMessage(long blockId) {
    return createBlockRequestMessage(blockId, 0, -1);
  }

  /**
   * Create a block request message for a part of the block by the block id, the offset and the
   * length. The message is ready to be sent. If <code>len</code> is -1, it means requesting the
   * data from offset to the end of the block.
   *
   * @param blockId The id of the block
   * @param offset The requested data's offset in the block
   * @param len The length of the requested data. If it's -1, it means request the data from offset
   *        to the block's end.
   * @return The created block request message
   */
  public static DataServerMessage createBlockRequestMessage(long blockId, long offset, long len) {
    DataServerMessage ret = new DataServerMessage(true, DATA_SERVER_REQUEST_MESSAGE);
    ret.mRPCMessageType = RPCMessage.Type.RPC_BLOCK_REQUEST;

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
   * @param toSend If true the message is to send the data, otherwise it's used to receive data.
   * @param blockId The id of the block
   * @param data The data of the message
   * @return The created block response message
   */
  public static DataServerMessage createBlockResponseMessage(boolean toSend, long blockId,
      ByteBuffer data) {
    return createBlockResponseMessage(toSend, blockId, 0, -1, data);
  }

  /**
   * Create a block response message specified by the block's id, the offset and the length. If
   * <code>toSend</code> is true, it will prepare the data to be sent, otherwise the message is used
   * to receive data. If <code>len</code> is -1, it means response the data from offset to the
   * block's end.
   *
   * @param toSend If true the message is to send the data, otherwise it's used to receive data
   * @param blockId The id of the block
   * @param offset The responded data's offset in the block
   * @param len The length of the responded data. If it's -1, it means respond the data from offset
   *        to the block's end.
   * @param data The data of the message
   * @return The created block response message
   */
  public static DataServerMessage createBlockResponseMessage(boolean toSend, long blockId,
      long offset, long len, ByteBuffer data) {
    DataServerMessage ret = new DataServerMessage(toSend, DATA_SERVER_RESPONSE_MESSAGE);
    ret.mRPCMessageType = RPCMessage.Type.RPC_BLOCK_RESPONSE;

    if (toSend) {
      if (data != null) {
        ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
        ret.mBlockId = blockId;
        ret.mOffset = offset;
        ret.mLength = len;
        ret.mData = data;
        ret.mIsMessageReady = true;
        ret.generateHeader();
      } else {
        // TODO This is a trick for now. The data may have been removed before remote retrieving.
        ret.mBlockId = -blockId;
        ret.mLength = 0;
        ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
        ret.mData = ByteBuffer.allocate(0);
        ret.mIsMessageReady = true;
        ret.generateHeader();
        LOG.error("The file is not here! blockId:{}", blockId);
      }
    } else {
      ret.mHeader = ByteBuffer.allocate(HEADER_LENGTH);
      ret.mData = null;
    }

    return ret;
  }

  private final boolean mToSendData;
  private final short mMessageType;
  private boolean mIsMessageReady;

  private ByteBuffer mHeader;

  private long mBlockId;

  private long mOffset;

  private long mLength;

  // TODO: Investigate how to remove this since it is not transferred over the wire.
  private long mLockId = -1L;

  private ByteBuffer mData = null;

  // This is the new message type. For now, DataServerMessage must manually send this type on the
  // network. When the client is converted to to use Netty, this DataServerMessage class will be
  // removed.
  private RPCMessage.Type mRPCMessageType;

  /**
   * New a DataServerMessage. Notice that it's not ready.
   *
   * @param isToSendData true if this is a send message, otherwise this is a recv message
   * @param msgType The message type
   */
  private DataServerMessage(boolean isToSendData, short msgType) {
    mToSendData = isToSendData;
    mMessageType = msgType;
    mIsMessageReady = false;
  }

  /**
   * Check if the message is ready. If not ready, it will throw a runtime exception.
   */
  public void checkReady() {
    Preconditions.checkState(mIsMessageReady, "Message is not ready.");
  }

  /**
   * Close the message.
   */
  public void close() {
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

    // These two fields are hard coded for now, until the client is converted to use netty.
    if (mMessageType == DATA_SERVER_REQUEST_MESSAGE) {
      mHeader.putLong(HEADER_LENGTH); // frame length
    } else {
      mHeader.putLong(HEADER_LENGTH + mLength); // frame length
    }
    mHeader.putInt(mRPCMessageType.getId()); // RPC message type

    mHeader.putShort(mMessageType);
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
  public long getLockId() {
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
    if (mToSendData != isSend) {
      if (mToSendData) {
        throw new RuntimeException("Try to recv on send message");
      } else {
        throw new RuntimeException("Try to send on recv message");
      }
    }
  }

  /**
   * Use this message to receive from the specified socket channel. Make sure this is a recv message
   * and the message type is matched.
   *
   * @param socketChannel The socket channel to receive from
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

        // These two fields are hard coded for now, until the client is converted to use netty.
        long frameLength = mHeader.getLong(); // frame length
        int msgRPCType = mHeader.getInt(); // RPC message type

        // TODO: this msgType will be replaced by the newer msgRPCType when the client is converted
        // to netty.
        short msgType = mHeader.getShort();
        assert (mMessageType == msgType);
        mBlockId = mHeader.getLong();
        mOffset = mHeader.getLong();
        mLength = mHeader.getLong();
        // TODO make this better to truncate the file.
        assert mLength < Integer.MAX_VALUE;
        if (mMessageType == DATA_SERVER_RESPONSE_MESSAGE) {
          if (mLength == -1) {
            mData = ByteBuffer.allocate(0);
          } else {
            mData = ByteBuffer.allocate((int) mLength);
          }
        }
        LOG.info("data {}, blockId:{} offset:{} dataLength:{}", mData, mBlockId, mOffset, mLength);
        if (mMessageType == DATA_SERVER_REQUEST_MESSAGE || mLength <= 0) {
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
   * @param socketChannel The socket channel to send to
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
   * @param lockId The id of the block's locker
   */
  public void setLockId(long lockId) {
    mLockId = lockId;
  }
}
