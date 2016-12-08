/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstract packet reader implementation with the actual reading part (the producer)
 * unimplemented.
*/
@NotThreadSafe
public abstract class AbstractPacketReader implements PacketReader {
  protected static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
  protected static final long READ_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  protected final InetSocketAddress mAddress;
  protected final long mId;
  private final long mStart;
  private final long mBytesToRead;

  protected ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  protected Queue<ByteBuf> mPackets = new LinkedList<>();
  @GuardedBy("mLock")
  protected Throwable mPacketReaderException = null;
  protected Condition mNotEmptyOrFail = mLock.newCondition();

  /** The next pos to read. */
  private long mPosToRead;
  /** This is true only when an empty packet is received. */
  protected boolean mDone = false;

  protected boolean mClosed = false;

  /**
   * Creates an instance of {@link NettyPacketReader}.
   *
   * @param address the Alluxio data server address
   * @param id the block ID or UFS ID
   * @param offset the offset
   * @param len the length to read
   * @throws IOException if it fails to create the object
   */
  protected AbstractPacketReader(InetSocketAddress address, long id, long offset, long len) {
    mAddress = address;
    mId = id;
    mStart = offset;
    mPosToRead = offset;
    mBytesToRead = len;
    Preconditions.checkState(offset >= 0 && len > 0);
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    Preconditions.checkState(!mClosed, "PacketReader is closed while reading packets.");
    ByteBuf buf = null;
    mLock.lock();
    try {
      while (true) {
        if (mDone) {
          return null;
        }
        if (mPacketReaderException != null) {
          throw new IOException(mPacketReaderException);
        }
        buf = mPackets.poll();
        if (!tooManyPacketsPending()) {
          resume();
        }
        if (buf == null) {
          try {
            if (!mNotEmptyOrFail.await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
              throw new IOException(
                  String.format("Timeout while reading packet from block %d @ %s.", mId, mAddress));
            }
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
        } else {
          if (buf.readableBytes() == 0) {
            buf.release();
            mDone = true;
            return null;
          }
          mPosToRead += buf.readableBytes();
          Preconditions.checkState(mPosToRead - mStart <= mBytesToRead);
          return buf;
        }
      }
    } catch (Throwable e) {
      if (buf != null) {
        buf.release();
      }
      throw e;
    } finally {
      mLock.unlock();
    }
  }

  /**
   * @return bytes remaining
   */
  protected long remaining() {
    return mStart + mBytesToRead - mPosToRead;
  }

  protected abstract void pause();

  protected abstract void resume();

  /**
   * @return true if there are too many packets pending
   */
  protected boolean tooManyPacketsPending() {
    return mPackets.size() >= MAX_PACKETS_IN_FLIGHT;
  }
}

