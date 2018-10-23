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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A packet writer that writes to local first and fallback to UFS block writes when the block
 * storage on this local worker is full.
 */
@NotThreadSafe
public final class UfsFallbackLocalFilePacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsFallbackLocalFilePacketWriter.class);
  private final PacketWriter mLocalFilePacketWriter;
  private final FileSystemContext mContext;
  private final WorkerNetAddress mWorkerNetAddress;
  private final long mBlockSize;
  private final long mBlockId;
  private final OutStreamOptions mOutStreamOptions;
  private NettyPacketWriter mNettyPacketWriter;
  private boolean mIsWritingToLocal;

  /**
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param blockSize the block size
   * @param options the output stream options
   * @return the {@link UfsFallbackLocalFilePacketWriter} instance created
   */
  public static UfsFallbackLocalFilePacketWriter create(FileSystemContext context,
      WorkerNetAddress address, long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    try {
      LocalFilePacketWriter localFilePacketWriter =
          LocalFilePacketWriter.create(context, address, blockId, options);
      return new UfsFallbackLocalFilePacketWriter(localFilePacketWriter, null, context, address,
          blockId, blockSize, options);
    } catch (ResourceExhaustedException e) {
      LOG.warn("Fallback to create new block {} in UFS due to a failure of insufficient space on "
          + "the local worker: {}", blockId, e.getMessage());
    }
    // Failed to create the local writer due to insufficient space, fallback to netty packet writer
    // directly
    NettyPacketWriter nettyPacketWriter = NettyPacketWriter
        .create(context, address, blockId, blockSize, Protocol.RequestType.UFS_FALLBACK_BLOCK,
            options);
    return new UfsFallbackLocalFilePacketWriter(null, nettyPacketWriter, context, address, blockId,
        blockSize, options);
  }

  UfsFallbackLocalFilePacketWriter(PacketWriter localFilePacketWriter,
      NettyPacketWriter nettyPacketWriter, FileSystemContext context,
      final WorkerNetAddress address, long blockId, long blockSize, OutStreamOptions options) {
    mLocalFilePacketWriter = localFilePacketWriter;
    mNettyPacketWriter = nettyPacketWriter;
    mBlockId = blockId;
    mContext = context;
    mWorkerNetAddress = address;
    mBlockSize = blockSize;
    mOutStreamOptions = options;
    mIsWritingToLocal = mLocalFilePacketWriter != null;
  }

  @Override
  public void writePacket(ByteBuf packet) throws IOException {
    if (mIsWritingToLocal) {
      long pos = mLocalFilePacketWriter.pos();
      try {
        // packet.refcount++ to ensure packet not garbage-collected if writePacket fails
        packet.retain();
        // packet.refcount-- inside regardless of exception
        mLocalFilePacketWriter.writePacket(packet);
        // packet.refcount-- on success
        packet.release();
        return;
      } catch (ResourceExhaustedException e) {
        LOG.warn("Fallback to write to UFS for block {} due to a failure of insufficient space "
            + "on the local worker: {}", mBlockId, e.getMessage());
        mIsWritingToLocal = false;
      }
      try {
        if (pos == 0) {
          // Nothing has been written to temp block, we can cancel this failed local writer and
          // cleanup the temp block.
          mLocalFilePacketWriter.cancel();
        } else {
          // Note that, we can not cancel mLocalFilePacketWriter now as the cancel message may
          // arrive and clean the temp block before it is written to UFS.
          mLocalFilePacketWriter.flush();
        }
        // Close the block writer. We do not close the mLocalFilePacketWriter to prevent the worker
        // completes the block, commit it and remove it.
        //mLocalFilePacketWriter.getWriter().close();
        mNettyPacketWriter = NettyPacketWriter
            .create(mContext, mWorkerNetAddress, mBlockId, mBlockSize,
                Protocol.RequestType.UFS_FALLBACK_BLOCK, mOutStreamOptions);
        // Instruct the server to write the previously transferred data from temp block to UFS only
        // when there is data already written.
        if (pos > 0) {
          mNettyPacketWriter.writeFallbackInitPacket(pos);
        }
      } catch (Exception e) {
        // packet.refcount-- on exception
        packet.release();
        throw new IOException("Failed to switch to writing block " + mBlockId + " to UFS", e);
      }
    }
    mNettyPacketWriter.writePacket(packet); // refcount-- inside to release packet
  }

  @Override
  public void flush() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFilePacketWriter.flush();
    } else {
      mNettyPacketWriter.flush();
    }
  }

  @Override
  public int packetSize() {
    if (mIsWritingToLocal) {
      return mLocalFilePacketWriter.packetSize();
    } else {
      return mNettyPacketWriter.packetSize();
    }
  }

  @Override
  public long pos() {
    if (mIsWritingToLocal) {
      return mLocalFilePacketWriter.pos();
    } else {
      return mNettyPacketWriter.pos();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFilePacketWriter.cancel();
    } else {
      // Clean up the state of previous temp block left over
      if (mLocalFilePacketWriter != null) {
        mLocalFilePacketWriter.cancel();
      }
      mNettyPacketWriter.cancel();
    }
  }

  @Override
  public void close() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFilePacketWriter.close();
    } else {
      // Clean up the state of previous temp block left over
      if (mLocalFilePacketWriter != null) {
        mLocalFilePacketWriter.cancel();
      }
      mNettyPacketWriter.close();
    }
  }
}
