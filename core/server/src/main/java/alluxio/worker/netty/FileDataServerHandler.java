/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.worker.file.FileSystemWorker;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This class handles filesystem data server requests.
 */
public class FileDataServerHandler {
  /** Filesystem worker which handles file level operations for the worker. */
  private final FileSystemWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  /**
   * Constructs a file data server handler for serving any ufs read/write requests.
   *
   * @param worker the file system worker
   * @param configuration the configuration to use
   */
  public FileDataServerHandler(FileSystemWorker worker, Configuration configuration) {
    mWorker = worker;
    mTransferType = configuration.getEnum(Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE,
        FileTransferType.class);
  }

  public void handleFileReadRequest(ChannelHandlerContext ctx, RPCFileReadRequest req)
      throws IOException {
    req.validate();

    long ufsFileId = req.getTempUfsFileId();
    long length = req.getLength();
    long offset = req.getOffset();

    InputStream in = mWorker.getUfsInputStream(ufsFileId);

  }

  private DataBuffer getDatabuffer(RPCFileReadRequest req, InputStream in, long offset) {
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = in
    }
  }
}
