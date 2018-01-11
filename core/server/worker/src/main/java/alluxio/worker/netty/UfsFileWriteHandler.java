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
import alluxio.underfs.UfsSessionManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.WorkerUfsSessionManager;
import alluxio.underfs.options.CreateOptions;

import com.codahale.metrics.Counter;
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
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class UfsFileWriteHandler extends AbstractWriteHandler<UfsFileWriteRequestContext> {
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
  protected UfsFileWriteRequestContext createRequestContext(Protocol.WriteRequest msg) {
    return new UfsFileWriteRequestContext(msg);
  }

  @Override
  protected void initRequestContext(UfsFileWriteRequestContext context) {
  }

  @Override
  protected PacketWriter createPacketWriter(UfsFileWriteRequestContext context, Channel channel) {
    return new UfsFilePacketWriter(context, channel, mUfsManager);
  }

  /**
   * The packet writer that writes to UFS.
   */
  public class UfsFilePacketWriter extends PacketWriter {
    private final UfsManager mUfsManager;
    private final UfsSessionManager mUfsSessionManager;

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param ufsManager UFS manager
     */
    public UfsFilePacketWriter(UfsFileWriteRequestContext context, Channel channel,
        UfsManager ufsManager) {
      super(context, channel);
      mUfsManager = ufsManager;
      mUfsSessionManager = new WorkerUfsSessionManager(ufsManager);
    }

    @Override
    protected void completeRequest(UfsFileWriteRequestContext context, Channel channel)
        throws Exception {
      if (context == null) {
        return;
      }
      if (context.getOutputStream() == null) {
        createUfsFile(context, channel);
      }
      Preconditions.checkState(context.getOutputStream() != null);
      context.getOutputStream().close();
      context.setOutputStream(null);
      mUfsSessionManager.closeSession(context.getRequest().getCreateUfsFileOptions().getMountId());
    }

    @Override
    protected void cancelRequest(UfsFileWriteRequestContext context) throws Exception {
      if (context == null) {
        return;
      }
      UfsFileWriteRequest request = context.getRequest();
      // TODO(calvin): Consider adding cancel to the ufs stream api.
      if (context.getOutputStream() != null && context.getUnderFileSystem() != null) {
        context.getOutputStream().close();
        context.getUnderFileSystem().deleteFile(request.getUfsPath());
        context.setOutputStream(null);
      }
      mUfsSessionManager.closeSession(context.getRequest().getCreateUfsFileOptions().getMountId());
    }

    @Override
    protected void cleanupRequest(UfsFileWriteRequestContext context) throws Exception {
      cancelRequest(context);
    }

    @Override
    protected void writeBuf(UfsFileWriteRequestContext context, Channel channel, ByteBuf buf,
        long pos) throws Exception {
      Preconditions.checkState(context != null);
      if (context.getOutputStream() == null) {
        createUfsFile(context, channel);
      }

      buf.readBytes(context.getOutputStream(), buf.readableBytes());
    }

    private void createUfsFile(UfsFileWriteRequestContext context, Channel channel)
        throws IOException {
      UfsFileWriteRequest request = context.getRequest();
      Preconditions.checkState(request != null);
      Protocol.CreateUfsFileOptions createUfsFileOptions = request.getCreateUfsFileOptions();
      UfsInfo ufsInfo = mUfsManager.get(createUfsFileOptions.getMountId());
      UnderFileSystem ufs = ufsInfo.getUfs();
      context.setUnderFileSystem(ufs);
      context.setOutputStream(ufs.create(request.getUfsPath(),
          CreateOptions.defaults().setOwner(createUfsFileOptions.getOwner())
              .setGroup(createUfsFileOptions.getGroup())
              .setMode(new Mode((short) createUfsFileOptions.getMode()))));
      String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
      String metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
      Counter counter = MetricsSystem.workerCounter(metricName);
      context.setCounter(counter);
      mUfsSessionManager.openSession(context.getRequest().getCreateUfsFileOptions().getMountId());
    }
  }
}
