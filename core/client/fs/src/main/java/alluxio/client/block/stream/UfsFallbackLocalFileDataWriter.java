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
import alluxio.grpc.RequestType;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data writer that writes to local first and fallback to UFS block writes when the block
 * storage on this local worker is full.
 */
@NotThreadSafe
public final class UfsFallbackLocalFileDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsFallbackLocalFileDataWriter.class);
  private final DataWriter mLocalFileDataWriter;
  private final FileSystemContext mContext;
  private final WorkerNetAddress mWorkerNetAddress;
  private final long mBlockSize;
  private final long mBlockId;
  private final OutStreamOptions mOutStreamOptions;
  private GrpcDataWriter mGrpcDataWriter;
  private boolean mIsWritingToLocal;

  /**
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param blockSize the block size
   * @param options the output stream options
   * @return the {@link UfsFallbackLocalFileDataWriter} instance created
   */
  public static UfsFallbackLocalFileDataWriter create(FileSystemContext context,
      WorkerNetAddress address, long blockId, long blockSize, OutStreamOptions options)
      throws IOException {
    try {
      LocalFileDataWriter localFilePacketWriter =
          LocalFileDataWriter.create(context, address, blockId, blockSize, options);
      return new UfsFallbackLocalFileDataWriter(localFilePacketWriter, null, context, address,
          blockId, blockSize, options);
    } catch (ResourceExhaustedException e) {
      LOG.warn("Fallback to create new block {} in UFS due to a failure of insufficient space on "
          + "the local worker: {}", blockId, e.toString());
    }
    // Failed to create the local writer due to insufficient space, fallback to gRPC data writer
    // directly
    GrpcDataWriter grpcDataWriter = GrpcDataWriter
        .create(context, address, blockId, blockSize, RequestType.UFS_FALLBACK_BLOCK,
            options);
    return new UfsFallbackLocalFileDataWriter(null, grpcDataWriter, context, address, blockId,
        blockSize, options);
  }

  UfsFallbackLocalFileDataWriter(DataWriter localFileDataWriter,
      GrpcDataWriter grpcDataWriter, FileSystemContext context,
      final WorkerNetAddress address, long blockId, long blockSize, OutStreamOptions options) {
    mLocalFileDataWriter = localFileDataWriter;
    mGrpcDataWriter = grpcDataWriter;
    mBlockId = blockId;
    mContext = context;
    mWorkerNetAddress = address;
    mBlockSize = blockSize;
    mOutStreamOptions = options;
    mIsWritingToLocal = mLocalFileDataWriter != null;
  }

  @Override
  public void writeChunk(ByteBuf chunk) throws IOException {
    if (mIsWritingToLocal) {
      long pos = mLocalFileDataWriter.pos();
      try {
        // chunk.refcount++ to ensure chunk not garbage-collected if writeChunk fails
        chunk.retain();
        // chunk.refcount-- inside regardless of exception
        mLocalFileDataWriter.writeChunk(chunk);
        // chunk.refcount-- on success
        chunk.release();
        return;
      } catch (ResourceExhaustedException e) {
        LOG.warn("Fallback to write to UFS for block {} due to a failure of insufficient space "
            + "on the local worker: {}", mBlockId, e.toString());
        mIsWritingToLocal = false;
      }
      try {
        if (pos == 0) {
          // Nothing has been written to temp block, we can cancel this failed local writer and
          // cleanup the temp block.
          mLocalFileDataWriter.cancel();
        } else {
          // Note that, we can not cancel mLocalFileDataWriter now as the cancel message may
          // arrive and clean the temp block before it is written to UFS.
          mLocalFileDataWriter.flush();
        }
        // Close the block writer. We do not close the mLocalFileDataWriter to prevent the worker
        // completes the block, commit it and remove it.
        //mLocalFileDataWriter.getWriter().close();
        mGrpcDataWriter = GrpcDataWriter
            .create(mContext, mWorkerNetAddress, mBlockId, mBlockSize,
                RequestType.UFS_FALLBACK_BLOCK, mOutStreamOptions);
        // Instruct the server to write the previously transferred data from temp block to UFS only
        // when there is data already written.
        if (pos > 0) {
          mGrpcDataWriter.writeFallbackInitRequest(pos);
        }
      } catch (Exception e) {
        // chunk.refcount-- on exception
        chunk.release();
        throw new IOException("Failed to switch to writing block " + mBlockId + " to UFS", e);
      }
    }
    mGrpcDataWriter.writeChunk(chunk); // refcount-- inside to release chunk
  }

  @Override
  public void flush() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFileDataWriter.flush();
    } else {
      mGrpcDataWriter.flush();
    }
  }

  @Override
  public int chunkSize() {
    if (mIsWritingToLocal) {
      return mLocalFileDataWriter.chunkSize();
    } else {
      return mGrpcDataWriter.chunkSize();
    }
  }

  @Override
  public long pos() {
    if (mIsWritingToLocal) {
      return mLocalFileDataWriter.pos();
    } else {
      return mGrpcDataWriter.pos();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFileDataWriter.cancel();
    } else {
      // Clean up the state of previous temp block left over
      if (mLocalFileDataWriter != null) {
        mLocalFileDataWriter.cancel();
      }
      mGrpcDataWriter.cancel();
    }
  }

  @Override
  public void close() throws IOException {
    if (mIsWritingToLocal) {
      mLocalFileDataWriter.close();
    } else {
      // Clean up the state of previous temp block left over
      if (mLocalFileDataWriter != null) {
        mLocalFileDataWriter.cancel();
      }
      mGrpcDataWriter.close();
    }
  }
}
