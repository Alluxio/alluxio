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

package tachyon.client.rdma;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.accelio.jxio.jxioConnection.JxioConnection;

import tachyon.Constants;
import tachyon.client.RemoteBlockReader;
import tachyon.conf.TachyonConf;
import tachyon.worker.DataServerMessage;

public final class RDMARemoteBlockReader implements RemoteBlockReader {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private int mClientBufCount = 10;
  private int mClientMsgPoolSize;
  private static final String URI_FORMAT = "://%s:%d/?blockId=%d&offset=%d&length=%d";
  private final TachyonConf mConf;

  public RDMARemoteBlockReader(TachyonConf conf) {
    this.mConf = conf;
    mClientBufCount = mConf.getInt(Constants.USER_RDMA_CLIENT_BUFFERS, mClientBufCount);
    mClientMsgPoolSize =
        org.accelio.jxio.jxioConnection.Constants.MSGPOOL_BUF_SIZE * mClientBufCount;
  }

  @Override
  public ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException {
    DataServerMessage remoteMsg = getRemoteMsg(host, port, blockId, offset, length);

    if (!remoteMsg.isMessageReady()) {
      LOG.info("Data {} from remote machine is not ready.", blockId);
      return null;
    }

    if (remoteMsg.getBlockId() < 0) {
      LOG.info("Data {} is not in remote machine.", remoteMsg.getBlockId());
      return null;
    }

    return remoteMsg.getReadOnlyData();
  }

  @Override
  public DataServerMessage getRemoteMsg(String host, int port, long blockId, long offset,
      long length) throws IOException {
    try {
      URI uri =
          new URI(String.format(mConf.get(Constants.JXIO_TRANSPORT, "rdma") + URI_FORMAT, host,
              port, blockId, offset, length));
      JxioConnection jc = new JxioConnection(uri);
      try {
        jc.setRcvSize(mClientMsgPoolSize); // 10 buffers in msg pool
        InputStream input = jc.getInputStream();
        LOG.info("Connected to remote machine {}", uri);

        DataServerMessage recvMsg =
            DataServerMessage.createBlockResponseMessage(false, blockId, null);
        recvMsg.recv(input);
        LOG.info("Data {} from remote machine {}:{} received", blockId, host, port);

        return recvMsg;

      } finally {
        jc.disconnect();
      }
    } catch (URISyntaxException e) {
      throw new IOException("rdma uri could not be resolved");
    }
  }
}
