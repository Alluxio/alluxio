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

import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.file.FileSystemWorker;

import com.codahale.metrics.Counter;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles file write request. Check more information in
 * {@link DataServerWriteHandler}.
 */
@NotThreadSafe
public final class DataServerFileWriteHandler extends DataServerWriteHandler {
  /** Filesystem worker which handles file level operations for the worker. */
  private final FileSystemWorker mWorker;

  private class FileWriteRequestInternal extends WriteRequestInternal {
    public OutputStream mOutputStream;

    public FileWriteRequestInternal(Protocol.WriteRequest request) throws Exception {
      mOutputStream = mWorker.getUfsOutputStream(request.getId());
      mId = request.getId();
    }

    @Override
    public void close() throws IOException {}
  }

  /**
   * Creates an instance of {@link DataServerFileWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param worker the file system worker
   */
  public DataServerFileWriteHandler(ExecutorService executorService, FileSystemWorker worker) {
    super(executorService);
    mWorker = worker;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = (Protocol.WriteRequest) ((RPCProtoMessage) object).getMessage();
    return request.getType() == Protocol.RequestType.UFS_FILE;
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  @Override
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    super.initializeRequest(msg);
    if (mRequest == null) {
      mRequest = new FileWriteRequestInternal((Protocol.WriteRequest) msg.getMessage());
    }
  }

  @Override
  protected void writeBuf(ByteBuf buf, long pos) throws Exception {
    buf.readBytes(((FileWriteRequestInternal) mRequest).mOutputStream, buf.readableBytes());
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    Metrics.BYTES_WRITTEN_UFS.inc(bytesWritten);
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_WRITTEN_UFS = MetricsSystem.workerCounter("BytesWrittenUFS");

    private Metrics() {
    } // prevent instantiation
  }
}
