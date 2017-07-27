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
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsInfo;
import alluxio.underfs.options.CreateOptions;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles writes to a file in the under file system. Due to the semantics enforced
 * on under file systems, the client must write all the data of a file through the same stream to
 * the under file system. This prevents us from handling the writes at a block level.
 *
 * For more information about the implementation of read/write buffering, see
 * {@link AbstractWriteHandler}.
 *
 * For more information about the implementation of the client side writer, see
 * UnderFileSystemFileOutStream.
 */
@NotThreadSafe
final class UfsFileWriteHandler extends AbstractWriteHandler {
  private final UfsManager mUfsManager;

  /**
   * Creates an instance of {@link UfsFileWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param ufsManager the file data manager
   */
  UfsFileWriteHandler(ExecutorService executorService, UfsManager ufsManager) {
    super(executorService);
    mUfsManager = ufsManager;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.UFS_FILE;
  }

  @Override
  protected AbstractWriteRequest createWriteRequest(RPCProtoMessage msg) throws Exception {
    return new UfsFileWriteRequest(this, msg.getMessage().asWriteRequest());
  }

  @Override
  protected void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
    UfsFileWriteRequest request = (UfsFileWriteRequest) getRequest();
    Preconditions.checkState(request != null);
    if (request.getOutputStream() == null) {
      createUfsFile(channel);
    }

    buf.readBytes(((UfsFileWriteRequest) getRequest()).getOutputStream(), buf.readableBytes());
  }

  void createUfsFile(Channel channel) throws IOException {
    UfsFileWriteRequest request = (UfsFileWriteRequest) getRequest();
    Preconditions.checkState(request != null);
    Protocol.CreateUfsFileOptions createUfsFileOptions = request.getCreateUfsFileOptions();
    UfsInfo ufsInfo = mUfsManager.get(createUfsFileOptions.getMountId());
    request.setUnderFileSystem(ufsInfo.getUfs());
    request.setOutputStream(request.getUnderFileSystem().create(request.getUfsPath(),
        CreateOptions.defaults().setOwner(createUfsFileOptions.getOwner())
            .setGroup(createUfsFileOptions.getGroup())
            .setMode(new Mode((short) createUfsFileOptions.getMode()))));
    String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
    String metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
    request.setCounter(MetricsSystem.workerCounter(metricName));
  }
}
