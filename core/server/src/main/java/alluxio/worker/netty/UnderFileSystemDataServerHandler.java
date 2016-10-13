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
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.RPCFileReadResponse;
import alluxio.network.protocol.RPCFileWriteRequest;
import alluxio.network.protocol.RPCFileWriteResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.worker.file.FileSystemWorker;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles filesystem data server requests.
 */
@NotThreadSafe
final class UnderFileSystemDataServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Filesystem worker which handles file level operations for the worker. */
  private final FileSystemWorker mWorker;

  /**
   * Constructs a file data server handler for serving any ufs read/write requests.
   *
   * @param worker the file system worker
   */
  public UnderFileSystemDataServerHandler(FileSystemWorker worker) {
    mWorker = worker;
  }

  /**
   * Handles a {@link RPCFileReadRequest} by reading the data through an input stream provided by
   * the file worker. This method assumes the length to read is less than or equal to the unread
   * data in the file.
   *
   * @param ctx The context of this request which handles the result of this operation
   * @param req The initiating {@link RPCFileReadRequest}
   * @throws IOException if an I/O error occurs when interacting with the UFS
   */
  public void handleFileReadRequest(ChannelHandlerContext ctx, RPCFileReadRequest req)
      throws IOException {
    req.validate();

    long ufsFileId = req.getTempUfsFileId();
    long offset = req.getOffset();
    long length = req.getLength();
    byte[] data = new byte[(int) length];

    try {
      InputStream in = mWorker.getUfsInputStream(ufsFileId, offset);
      int bytesRead = 0;
      if (in != null) { // if we have not reached the end of the file
        while (bytesRead < length) {
          // TODO(peis): Fix this. It is not recommended to do heavy blocking IO operation
          // in Netty's IO event group. We should do it in another threadpool.
          int read = in.read(data, bytesRead, (int) length - bytesRead);
          if (read == -1) {
            break;
          }
          bytesRead += read;
        }
      }
      DataBuffer buf =
          bytesRead != 0 ? new DataByteBuffer(ByteBuffer.wrap(data, 0, bytesRead), bytesRead)
              : null;
      RPCFileReadResponse resp =
          new RPCFileReadResponse(ufsFileId, offset, bytesRead, buf, RPCResponse.Status.SUCCESS);
      ctx.writeAndFlush(resp);
    } catch (Exception e) {
      // TODO(peis): Fix this. The exception here should never be caused netty related issue.
      LOG.error("Failed to read ufs file, may have been closed due to a client timeout.", e);
      RPCFileReadResponse resp =
          RPCFileReadResponse.createErrorResponse(req, RPCResponse.Status.UFS_READ_FAILED);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Handles a {@link RPCFileWriteRequest} by writing the data through an output stream provided
   * by the file worker. This method only allows appending data to the file and does not support
   * writing at arbitrary offsets.
   *
   * @param ctx The context of this request which handles the result of this operation
   * @param req The initiating {@link RPCFileWriteRequest}
   * @throws IOException if an I/O error occurs when interacting with the UFS
   */
  public void handleFileWriteRequest(ChannelHandlerContext ctx, RPCFileWriteRequest req)
      throws IOException {
    long ufsFileId = req.getTempUfsFileId();
    // Currently unused as only sequential write is supported
    long offset = req.getOffset();
    long length = req.getLength();
    final DataBuffer data = req.getPayloadDataBuffer();

    try {
      OutputStream out = mWorker.getUfsOutputStream(ufsFileId);
      // This channel will not be closed because the underlying stream should not be closed, the
      // channel will be cleaned up when the underlying stream is closed.
      WritableByteChannel channel = Channels.newChannel(out);
      channel.write(data.getReadOnlyByteBuffer());
      RPCFileWriteResponse resp =
          new RPCFileWriteResponse(ufsFileId, offset, length, RPCResponse.Status.SUCCESS);
      ctx.writeAndFlush(resp);
    } catch (Exception e) {
      // TODO(peis): Fix this. The exception here should never be caused netty related issue.
      LOG.error("Failed to write ufs file.", e);
      RPCFileWriteResponse resp =
          RPCFileWriteResponse.createErrorResponse(req, RPCResponse.Status.UFS_WRITE_FAILED);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }
}
