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

package alluxio.worker.block.io;

import alluxio.io.Cancelable;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.util.LogUtils;
import alluxio.util.SessionIdUtils;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data writer that interacts with block worker operations
 * to write data to worker storage.
 */
@NotThreadSafe
public final class WorkerBlockWriter extends BlockWriter implements Closeable, Cancelable {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerBlockWriter.class);

  private final long mBlockId;
  private final BlockWorker mBlockWorker;
  private final long mFileBufferBytes;
  private final long mSessionId;

  private BlockWriter mBlockWriter;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  private boolean mPinOnCreate;

  /**
   * Creates an instance of {@link WorkerBlockWriter}.
   *
   * @param blockWorker the blockWorker
   * @param blockId the block id
   * @param writeTier the write tier
   * @param mediumType the medium type
   * @param bytesToReserve bytes to reserve
   * @param pinOnCreate whether to pin on file creation
   * @return the {@link WorkerBlockWriter} created
   */
  public static WorkerBlockWriter create(BlockWorker blockWorker, long blockId, int writeTier,
      String mediumType, long bytesToReserve, boolean pinOnCreate) throws IOException {
    long sessionId = SessionIdUtils.createSessionId();
    try {
      blockWorker.createBlockRemote(sessionId, blockId, writeTier, mediumType, bytesToReserve);
      return new WorkerBlockWriter(blockWorker, sessionId, blockId, bytesToReserve, pinOnCreate);
    } catch (Exception e) {
      LogUtils.warnWithException(LOG,
          "Exception occurred when creating block for writing [sessionId: {}, blockId: {}]",
          sessionId, blockId, e);
      blockWorker.cleanupSession(sessionId);
      throw new IOException(e);
    }
  }

  @Override
  public long getPosition() {
    return mPos;
  }

  @Override
  public long append(final ByteBuffer buf) throws IOException {
    try {
      long size = buf.limit() - buf.position();
      requestSpaceAndGetWriter(size);
      Preconditions.checkState(mBlockWriter.append(buf) == size);
      return size;
    } catch (Exception e) {
      logWriteExceptionAndCleanup(e);
      throw new IOException(e);
    } finally {
      // TODO(lu) is this the right way to release ByteBuffer
      buf.clear();
    }
  }

  @Override
  public long append(final ByteBuf buf) throws IOException {
    try {
      long size = buf.readableBytes();
      requestSpaceAndGetWriter(size);
      Preconditions.checkState(mBlockWriter.append(buf) == size);
      return size;
    } catch (Exception e) {
      logWriteExceptionAndCleanup(e);
      throw new IOException(e);
    } finally {
      buf.release();
    }
  }

  @Override
  public long append(final DataBuffer buf) throws IOException {
    try {
      long size = buf.readableBytes();
      requestSpaceAndGetWriter(size);
      Preconditions.checkState(mBlockWriter.append(buf) == size);
      return size;
    } catch (Exception e) {
      logWriteExceptionAndCleanup(e);
      throw new IOException(e);
    } finally {
      buf.release();
    }
  }

  @Override
  public WritableByteChannel getChannel() {
    throw new UnsupportedOperationException("GetChannel is not supported");
  }

  @Override
  public void cancel() throws IOException {
    if (mBlockWriter != null) {
      mBlockWriter.close();
    }
    try {
      mBlockWorker.abortBlock(mSessionId, mBlockId);
    } catch (Exception e) {
      LogUtils.warnWithException(LOG,
          "Exception occurred when cenceling the write request [sessionId: {}, blockId: {}]",
          mSessionId, mBlockId, e);
      cleanup();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (mBlockWriter != null) {
      mBlockWriter.close();
    }
    try {
      mBlockWorker.commitBlock(mSessionId, mBlockId, mPinOnCreate);
    } catch (Exception e) {
      LogUtils.warnWithException(LOG,
          "Exception occurred when completing the write request [sessionId: {}, blockId: {}]",
          mSessionId, mBlockId, e);
      cleanup();
      throw new IOException(e);
    }
  }

  /**
   * Cleans up the write if any exception occurs.
   */
  public void cleanup() {
    try {
      if (mBlockWriter != null) {
        mBlockWriter.close();
      }
      mBlockWorker.cleanupSession(mSessionId);
    } catch (Exception e) {
      LOG.warn("Failed to cleanup states of writing [sessionId: {}, blockId: {}] with error {}",
          mSessionId, mBlockId, e.getMessage());
    }
  }

  private void requestSpaceAndGetWriter(long size) throws Exception {
    if (mPosReserved < mPos + size) {
      long bytesToReserve = Math.max(mFileBufferBytes, mPos + size - mPosReserved);
      // Allocate enough space in the existing temporary block for the write.
      mBlockWorker.requestSpace(mSessionId, mBlockId, bytesToReserve);
      mPosReserved += bytesToReserve;
    }
    if (mBlockWriter == null) {
      mBlockWriter = mBlockWorker.getTempBlockWriterRemote(mSessionId, mBlockId);
    }
    Preconditions.checkState(mBlockWriter != null);
    mPos += size;
  }

  private void logWriteExceptionAndCleanup(Exception e) {
    LogUtils.warnWithException(LOG, "Exception occurred when writing [sessionId: {}, blockId: {}]",
        mSessionId, mBlockId, e);
    cleanup();
  }

  /**
   * Creates an instance of {@link WorkerBlockWriter}.
   *
   * @param blockWorker the block worker
   * @param sessionId the session id
   * @param blockId id the block id
   * @param fileBufferBytes the file buffer size in bytes
   * @param pinOnCreate whether to pin on create
   */
  private WorkerBlockWriter(BlockWorker blockWorker,
      long sessionId, long blockId, long fileBufferBytes, boolean pinOnCreate) {
    mBlockWorker = blockWorker;
    mSessionId = sessionId;
    mBlockId = blockId;
    mFileBufferBytes = fileBufferBytes;
    mPinOnCreate = pinOnCreate;
  }
}
