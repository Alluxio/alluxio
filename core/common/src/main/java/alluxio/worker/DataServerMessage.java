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

package alluxio.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

/**
 * The message type used to send data request and response for remote data.
 */
@NotThreadSafe
public final class DataServerMessage {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // The size of the prefix of the header: frame length (long), messageType (int)
  private static final int HEADER_PREFIX_LENGTH = 12;
  // The request header is: HEADER_PREFIX, blockId (long), offset (long), length (long),
  // lockId (long), sessionId (long)
  private static final int REQUEST_HEADER_LENGTH = HEADER_PREFIX_LENGTH + 40;
  // The response header is: HEADER_PREFIX, blockId (long), offset (long), length (long),
  // status (short)
  private static final int RESPONSE_HEADER_LENGTH = HEADER_PREFIX_LENGTH + 26;
  // The error response header is: HEADER_PREFIX, status (short)
  private static final int ERROR_RESPONSE_HEADER_LENGTH = HEADER_PREFIX_LENGTH + 2;

  /**
   * Creates a default block request message, just allocates the message header, and no attribute is
   * set. The message is not ready to be sent.
   *
   * @return the created block request message
   */
  public static DataServerMessage createBlockRequestMessage() {
    DataServerMessage ret = new DataServerMessage(false, RPCMessage.Type.RPC_BLOCK_READ_REQUEST);
    ret.mHeader = ByteBuffer.allocate(REQUEST_HEADER_LENGTH);
    return ret;
  }

  /**
   * Creates a block request message for a part of the block by the block id, the offset and the
   * length. The message is ready to be sent. If {@code len} is -1, it means requesting the data
   * from offset to the end of the block.
   *
   * @param blockId The id of the block
   * @param offset The requested data's offset in the block
   * @param len The length of the requested data. If it's -1, it means request the data from offset
   *        to the block's end.
   * @param lockId The lockId of the locked block
   * @param sessionId The id of requester's session
   * @return The created block request message
   */
  public static DataServerMessage createBlockRequestMessage(long blockId, long offset, long len,
      long lockId, long sessionId) {
    DataServerMessage ret = new DataServerMessage(true, RPCMessage.Type.RPC_BLOCK_READ_REQUEST);

    ret.mHeader = ByteBuffer.allocate(REQUEST_HEADER_LENGTH);
    ret.mBlockId = blockId;
    ret.mOffset = offset;
    ret.mLength = len;
    ret.mLockId = lockId;
    ret.mSessionId = sessionId;
    ret.generateHeader();
    ret.mData = ByteBuffer.allocate(0);
    ret.mIsMessageReady = true;

    return ret;
  }

  /**
   * Creates a block response message specified by the block's id. If {@code toSend} is true, it
   * will prepare the data to be sent, otherwise the message is used to receive data.
   *
   * @param toSend if true the message is to send the data, otherwise it's used to receive data
   * @param blockId The id of the block
   * @param data The data of the message
   * @return The created block response message
   */
  public static DataServerMessage createBlockResponseMessage(boolean toSend, long blockId,
      ByteBuffer data) {
    return createBlockResponseMessage(toSend, blockId, 0, -1, data);
  }

  /**
   * Creates a block response message specified by the block's id, the offset and the length. If
   * {@code toSend} is true, it will prepare the data to be sent, otherwise the message is used
   * to receive data. If {@code len} is -1, it means response the data from offset to the block's
   * end.
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
    DataServerMessage ret = new DataServerMessage(toSend, RPCMessage.Type.RPC_BLOCK_READ_RESPONSE);

    if (toSend) {
      if (data != null) {
        ret.mHeader = ByteBuffer.allocate(RESPONSE_HEADER_LENGTH);
        ret.mBlockId = blockId;
        ret.mOffset = offset;
        ret.mLength = len;
        ret.mStatus = RPCResponse.Status.SUCCESS;
        ret.mData = data;
        ret.mIsMessageReady = true;
        ret.generateHeader();
      } else {
        ret.mBlockId = blockId;
        ret.mLength = 0;
        ret.mHeader = ByteBuffer.allocate(RESPONSE_HEADER_LENGTH);
        ret.mData = ByteBuffer.allocate(0);
        ret.mIsMessageReady = true;
        ret.mStatus = RPCResponse.Status.FILE_DNE;
        LOG.error("The file is not here! blockId:{}", blockId);
        ret.generateHeader();
      }
    } else {
      ret.mHeader = ByteBuffer.allocate(RESPONSE_HEADER_LENGTH);
      ret.mData = null;
    }

    return ret;
  }

  private final boolean mToSendData;
  private final RPCMessage.Type mMessageType;
  private boolean mIsMessageReady;

  private ByteBuffer mHeader;

  private long mBlockId;

  private long mOffset;

  private long mLength;

  private RPCResponse.Status mStatus;

  private long mLockId = -1L;

  private long mSessionId;

  private ByteBuffer mData = null;

  /**
   * New a DataServerMessage. Notice that it's not ready.
   *
   * @param isToSendData true if this is a send message, otherwise this is a recv message
   * @param msgType The message type
   */
  private DataServerMessage(boolean isToSendData, RPCMessage.Type msgType) {
    mToSendData = isToSendData;
    mMessageType = msgType;
    mIsMessageReady = false;
  }

  /**
   * Checks if the message is ready. If not ready, it will throw a runtime exception.
   */
  public void checkReady() {
    Preconditions.checkState(mIsMessageReady, "Message is not ready.");
  }

  /**
   * Closes the message.
   */
  public void close() {
  }

  /**
   * Returns whether the message finishes sending or not. It will check if the message is a send
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
    // The header must match the Netty RPC messages.

    if (mMessageType == RPCMessage.Type.RPC_BLOCK_READ_REQUEST) {
      mHeader.putLong(REQUEST_HEADER_LENGTH); // frame length
    } else {
      // The response message has a payload.
      mHeader.putLong(RESPONSE_HEADER_LENGTH + mLength); // frame length
    }
    mHeader.putInt(mMessageType.getId()); // RPC message type

    mHeader.putLong(mBlockId);
    mHeader.putLong(mOffset);
    mHeader.putLong(mLength);

    if (mMessageType == RPCMessage.Type.RPC_BLOCK_READ_REQUEST) {
      // The request message has a lockId and a sessionId
      mHeader.putLong(mLockId);
      mHeader.putLong(mSessionId);
    } else if (mMessageType == RPCMessage.Type.RPC_BLOCK_READ_RESPONSE) {
      // The response message has a status.
      mHeader.putShort(mStatus.getId());
    }
    mHeader.flip();
  }

  /**
   * Gets the id of the block. Make sure the message is ready before calling this method.
   *
   * @return The id of the block
   */
  public long getBlockId() {
    checkReady();
    return mBlockId;
  }

  /**
   * Gets the length of the message's requested or responded data. Make sure the message is ready
   * before calling this method.
   *
   * @return The length of the message's requested or responded data
   */
  public long getLength() {
    checkReady();
    return mLength;
  }

  /**
   * Gets the id of the block's locker.
   *
   * @return The id of the block's locker
   */
  public long getLockId() {
    return mLockId;
  }

  /**
   * Gets the offset of the message's data in the block. Make sure the message is ready before
   * calling this method.
   *
   * @return The offset of the message's data in the block
   */
  public long getOffset() {
    checkReady();
    return mOffset;
  }

  /**
   * Gets the sessionId of the worker making the request. Make sure the message is ready before
   * calling this method.
   *
   * @return The session id of the requester
   */
  public long getSessionId() {
    checkReady();
    return mSessionId;
  }

  /**
   * Gets the status of the response. Make sure the message is ready before calling this method.
   *
   * @return The {@link alluxio.network.protocol.RPCResponse.Status} of the response
   */
  public RPCResponse.Status getStatus() {
    checkReady();
    return mStatus;
  }

  /**
   * Gets the read only buffer of the message's data. Make sure the message is ready before calling
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
   * @throws IOException when a non-Alluxio related exception occurs
   */
  public int recv(SocketChannel socketChannel) throws IOException {
    isSend(false);

    int numRead = 0;
    if (mHeader.remaining() > 0) {
      numRead = socketChannel.read(mHeader);
      if (numRead == -1 && mHeader.position() >= ERROR_RESPONSE_HEADER_LENGTH) {
        // Stream ended, but the full header is not received. This means an error response was
        // returned.
        mHeader.flip();
        // frame length
        mHeader.getLong();
        int receivedMessageType = mHeader.getInt();
        if (receivedMessageType == RPCMessage.Type.RPC_ERROR_RESPONSE.getId()) {
          // This message is expected to be an error response with a status.
          mStatus = RPCResponse.Status.fromShort(mHeader.getShort());
          throw new IOException(mStatus.getMessage());
        } else {
          // Unexpected message type.
          throw new IOException("Received an unexpected message type: " + receivedMessageType);
        }
      } else if (mHeader.remaining() == 0) {
        // The full header for a request message or a response message is received.
        mHeader.flip();

        // frame length
        mHeader.getLong();
        int receivedMessageType = mHeader.getInt();
        Preconditions.checkState(mMessageType.getId() == receivedMessageType,
            "Unexpected message type (" + receivedMessageType + ") received. expected: "
                + mMessageType.getId());
        mBlockId = mHeader.getLong();
        mOffset = mHeader.getLong();
        mLength = mHeader.getLong();
        if (mMessageType.getId() == RPCMessage.Type.RPC_BLOCK_READ_REQUEST.getId()) {
          // Additional fields for block read request
          mLockId = mHeader.getLong();
          mSessionId = mHeader.getLong();
        }
        // TODO(hy): Make this better to truncate the file.
        Preconditions.checkState(mLength < Integer.MAX_VALUE,
            "received length is too large: " + mLength);
        if (mMessageType == RPCMessage.Type.RPC_BLOCK_READ_RESPONSE) {
          // The response message has a status.
          mStatus = RPCResponse.Status.fromShort(mHeader.getShort());
          if (mStatus == RPCResponse.Status.SUCCESS) {
            mData = ByteBuffer.allocate((int) mLength);
          } else {
            mData = ByteBuffer.allocate(0);
          }
        }
        LOG.info("data {}, blockId:{} offset:{} dataLength:{}", mData, mBlockId, mOffset, mLength);
        if (mMessageType == RPCMessage.Type.RPC_BLOCK_READ_REQUEST) {
          mIsMessageReady = true;
        } else if (mMessageType == RPCMessage.Type.RPC_BLOCK_READ_RESPONSE
            && (mLength <= 0 || mStatus != RPCResponse.Status.SUCCESS)) {
          // There is no more to read from the socket.
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
   * Sends this message to the specified socket channel. Make sure this is a send message.
   *
   * @param socketChannel The socket channel to send to
   * @throws IOException when a non-Alluxio related exception occurs
   */
  public void send(SocketChannel socketChannel) throws IOException {
    Preconditions.checkNotNull(socketChannel);
    isSend(true);

    socketChannel.write(mHeader);

    if (mHeader.remaining() == 0) {
      socketChannel.write(mData);
    }
  }

  /**
   * Sets the id of the block's locker.
   *
   * @param lockId The id of the block's locker
   */
  public void setLockId(long lockId) {
    mLockId = lockId;
  }
}
