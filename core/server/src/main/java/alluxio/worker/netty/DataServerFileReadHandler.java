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
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.file.FileSystemWorker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles file read request. Check more information in {@link DataServerReadHandler}.
 */
@NotThreadSafe
public final class DataServerFileReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final FileSystemWorker mWorker;

  /**
   * The block read request internal representation.
   */
  private final class FileReadRequestInternal extends ReadRequestInternal {
    /** The UFS input stream. No need to close. */
    public InputStream mInputStream = null;

    /**
     * Creates an instance of {@link FileReadRequestInternal}.
     *
     * @param request the block read request
     * @throws Exception if it fails to create the object
     */
    public FileReadRequestInternal(Protocol.ReadRequest request) throws Exception {
      mInputStream = mWorker.getUfsInputStream(request.getId(), request.getOffset());

      mId = request.getId();
      mStart = request.getOffset();
      mEnd = mStart + request.getLength();
    }

    @Override
    public void close() {}
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   * @param worker the file system worker
   */
  public DataServerFileReadHandler(ExecutorService executorService, FileSystemWorker worker) {
    super(executorService);
    mWorker = worker;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.ReadRequest request = (Protocol.ReadRequest) ((RPCProtoMessage) object).getMessage();
    return request.getType() == Protocol.RequestType.UFS_FILE;
  }

  @Override
  protected void initializeRequest(Protocol.ReadRequest request) throws Exception {
    mRequest = new FileReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws IOException {
    ByteBuf buf = channel.alloc().buffer(len, len);
    try {
      InputStream in = ((FileReadRequestInternal) mRequest).mInputStream;
      if (in != null) { // if we have not reached the end of the file
        while (buf.writableBytes() > 0 && buf.writeBytes(in, buf.writableBytes()) != -1) {
        }
      }
      if (buf.readableBytes() == 0) {
        buf.release();
        return null;
      }
      return new DataNettyBufferV2(buf);
    } catch (Throwable e) {
      buf.release();
      throw e;
    }
  }
}
