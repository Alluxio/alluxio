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

import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(UfsFileWriteHandler.class);
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

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param ufsManager UFS manager
     */
    public UfsFilePacketWriter(UfsFileWriteRequestContext context, Channel channel,
        UfsManager ufsManager) {
      super(context, channel);
      mUfsManager = ufsManager;
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
      CreateOptions createOptions = context.getCreateOptions();
      if (createOptions != null) {
        try {
          // Set the owner/group of the file to the correct owner.
          context.getUfsResource().get()
              .setOwner(context.getRequest().getUfsPath(), createOptions.getOwner(),
                  createOptions.getGroup());
        } catch (IOException e) {
          LOG.warn("Failed to update ownership for ufs path: {} owner: {} group: {} error: {}",
              context.getRequest().getUfsPath(), createOptions.getOwner(), createOptions.getGroup(),
              e.toString());
        }
      }
      context.setOutputStream(null);
      context.setCreateOptions(null);
      context.getUfsResource().close();
    }

    @Override
    protected void cancelRequest(UfsFileWriteRequestContext context) throws Exception {
      if (context == null) {
        return;
      }
      UfsFileWriteRequest request = context.getRequest();
      // TODO(calvin): Consider adding cancel to the ufs stream api.
      if (context.getOutputStream() != null && context.getUfsResource() != null) {
        context.getOutputStream().close();
        context.getUfsResource().get().deleteFile(request.getUfsPath());
        context.setOutputStream(null);
        context.setCreateOptions(null);
        context.getUfsResource().close();
      }
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
      UfsManager.UfsClient ufsClient = mUfsManager.get(createUfsFileOptions.getMountId());
      CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource();
      context.setUfsResource(ufsResource);
      UnderFileSystem ufs = ufsResource.get();
      CreateOptions createOptions = CreateOptions.defaults()
          .setOwner(createUfsFileOptions.getOwner()).setGroup(createUfsFileOptions.getGroup())
          .setMode(new Mode((short) createUfsFileOptions.getMode()));
      context.setOutputStream(ufs.create(request.getUfsPath(), createOptions));
      context.setCreateOptions(createOptions);
      String ufsString = MetricsSystem.escape(ufsClient.getUfsMountPointUri());
      String counterName = Metric.getMetricNameWithTags(WorkerMetrics.BYTES_WRITTEN_UFS,
          WorkerMetrics.TAG_UFS, ufsString);
      Counter counter = MetricsSystem.counter(counterName);
      context.setCounter(counter);
      String meterName = Metric.getMetricNameWithTags(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT,
          WorkerMetrics.TAG_UFS, ufsString);
      context.setMeter(MetricsSystem.meter(meterName));
    }
  }
}
