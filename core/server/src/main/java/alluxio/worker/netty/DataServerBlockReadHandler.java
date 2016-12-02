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

package alluxio.worker.netty;

import alluxio.Constants;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockReadRequest}s.
 */
@NotThreadSafe
final public class DataServerBlockReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  /**
   * The block read request internal representation.
   */
  private final class BlockReadRequestInternal extends ReadRequestInternal {
    public BlockReader mBlockReader = null;

    /**
     * Creates an instance of {@link BlockReadRequestInternal}.
     *
     * @param request the block read request
     * @throws Exception if it fails to create the object
     */
    public BlockReadRequestInternal(RPCBlockReadRequest request) throws Exception {
      mBlockReader = mWorker
          .readBlockRemote(request.getSessionId(), request.getBlockId(), request.getLockId());
      mId = request.getBlockId();
      mWorker.accessBlock(request.getSessionId(), mId);

      mStart = request.getOffset();
      mEnd = mStart + request.getLength();
    }

    @Override
    public void close() {
      if (mBlockReader != null)  {
        try {
          mBlockReader.close();
        } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {}.", mId);
        }
      }
    }
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s.
   * @param blockWorker the block worker
   * @param fileTransferType the file transfer type
   */
  public DataServerBlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
      FileTransferType fileTransferType) {
    super(executorService);
    mWorker = blockWorker;
    mTransferType = fileTransferType;
  }

  @Override
  protected void initializeRequest(RPCBlockReadRequest request) throws Exception {
    mRequest = new BlockReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(long offset, int len) throws IOException {
    BlockReader blockReader = ((BlockReadRequestInternal) mRequest).mBlockReader;
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = blockReader.read(offset, len);
        return new DataByteBuffer(data, len);
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        Preconditions.checkArgument(blockReader.getChannel() instanceof FileChannel,
            "Only FileChannel is supported!");
        return new DataFileChannel((FileChannel) blockReader.getChannel(), offset, len);
    }
  }
}
