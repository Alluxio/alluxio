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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.NotFoundException;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.BlockUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles UFS block write request. Instead of writing a block to tiered storage, this
 * handler writes the block into UFS .
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class UfsFallbackBlockWriteHandler
    extends AbstractWriteHandler<BlockWriteRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(UfsFallbackBlockWriteHandler.class);
  private static final long FILE_BUFFER_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  private final UfsManager mUfsManager;
  private final BlockWriteHandler mBlockWriteHandler;

  /**
   * Creates an instance of {@link UfsFallbackBlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   */
  UfsFallbackBlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker,
      UfsManager ufsManager) {
    super(executorService);
    mWorker = blockWorker;
    mUfsManager = ufsManager;
    mBlockWriteHandler = new BlockWriteHandler(executorService, blockWorker);
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.UFS_FALLBACK_BLOCK;
  }

  @Override
  protected PacketWriter createPacketWriter(BlockWriteRequestContext context, Channel channel) {
    if (context.isWritingToLocal()) {
      //// not fallback yet, starting with the block packet writer
      return new UfsFallbackBlockPacketWriter(context, channel, mUfsManager,
          mBlockWriteHandler.createPacketWriter(context, channel));
    } else {
      return new UfsFallbackBlockPacketWriter(context, channel, mUfsManager, null);
    }
  }

  @Override
  protected BlockWriteRequestContext createRequestContext(Protocol.WriteRequest msg) {
    BlockWriteRequestContext context = new BlockWriteRequestContext(msg, FILE_BUFFER_SIZE);
    BlockWriteRequest request = context.getRequest();
    Preconditions.checkState(request.hasCreateUfsBlockOptions());
    // if it is already a UFS fallback from short-circuit write, avoid writing to local again
    context.setWritingToLocal(!request.getCreateUfsBlockOptions().getFallback());
    return context;
  }

  @Override
  protected void initRequestContext(BlockWriteRequestContext context) throws Exception {
    BlockWriteRequest request = context.getRequest();
    if (context.isWritingToLocal()) {
      mWorker.createBlockRemote(request.getSessionId(), request.getId(),
          mStorageTierAssoc.getAlias(request.getTier()), FILE_BUFFER_SIZE);
    }
  }

  /**
   * A packet writer that falls back to write the block to UFS in case the local block store is
   * full.
   */
  @NotThreadSafe
  public class UfsFallbackBlockPacketWriter extends PacketWriter {
    /** The block writer writing to the Alluxio storage of the local worker. */
    private final PacketWriter mBlockPacketWriter;
    private final UfsManager mUfsManager;

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param ufsManager UFS manager
     * @param blockPacketWriter local block store writer
     */
    public UfsFallbackBlockPacketWriter(BlockWriteRequestContext context, Channel channel,
        UfsManager ufsManager, PacketWriter blockPacketWriter) {
      super(context, channel);
      mBlockPacketWriter = blockPacketWriter;
      mUfsManager = Preconditions.checkNotNull(ufsManager);
    }

    @Override
    protected void completeRequest(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.completeRequest(context, channel);
      } else {
        mWorker.commitBlockInUfs(context.getRequest().getId(), context.getPosToQueue());
        if (context.getOutputStream() != null) {
          context.getOutputStream().close();
          context.setOutputStream(null);
        }
      }
      if (context.getUfsResource() != null) {
        context.getUfsResource().close();
      }
    }

    @Override
    protected void cancelRequest(BlockWriteRequestContext context) throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.cancelRequest(context);
      } else {
        if (context.getOutputStream() != null) {
          context.getOutputStream().close();
          context.setOutputStream(null);
        }
        if (context.getUfsResource() != null) {
          context.getUfsResource().get().deleteFile(context.getUfsPath());
        }
      }
      if (context.getUfsResource() != null) {
        context.getUfsResource().close();
      }
    }

    @Override
    protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.cleanupRequest(context);
      } else {
        cancelRequest(context);
      }
    }

    @Override
    protected void flushRequest(BlockWriteRequestContext context) throws Exception {
      if (context.isWritingToLocal()) {
        mBlockPacketWriter.flushRequest(context);
      } else if (context.getOutputStream() != null) {
        context.getOutputStream().flush();
      }
    }

    @Override
    protected void writeBuf(BlockWriteRequestContext context, Channel channel, ByteBuf buf,
        long pos) throws Exception {
      if (context.isWritingToLocal()) {
        // TODO(binfan): change signature of writeBuf to pass current offset and length of buffer.
        // Currently pos is the calculated offset after writeBuf succeeds.
        long posBeforeWrite = pos - buf.readableBytes();
        try {
          mBlockPacketWriter.writeBuf(context, channel, buf, pos);
          return;
        } catch (WorkerOutOfSpaceException e) {
          LOG.warn("Not enough space to write block {} to local worker, fallback to UFS. "
              + " {} bytes have been written.",
              context.getRequest().getId(), posBeforeWrite);
          context.setWritingToLocal(false);
        }
        // close the block writer first
        if (context.getBlockWriter() != null) {
          context.getBlockWriter().close();
        }
        // prepare the UFS block and transfer data from the temp block to UFS
        createUfsBlock(context, channel);
        if (posBeforeWrite > 0) {
          transferToUfsBlock(context, posBeforeWrite);
        }
        // close the original block writer and remove the temp file
        mBlockPacketWriter.cancelRequest(context);
      }
      if (context.getOutputStream() == null) {
        createUfsBlock(context, channel);
      }
      if (buf == AbstractWriteHandler.UFS_FALLBACK_INIT) {
        // transfer data from the temp block to UFS
        transferToUfsBlock(context, pos);
      } else {
        buf.readBytes(context.getOutputStream(), buf.readableBytes());
      }
    }

    /**
     * Creates a UFS block and initialize it with bytes read from block store.
     *
     * @param context context of this request
     * @param channel netty channel
     */
    private void createUfsBlock(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      BlockWriteRequest request = context.getRequest();
      Protocol.CreateUfsBlockOptions createUfsBlockOptions = request.getCreateUfsBlockOptions();
      UfsManager.UfsClient ufsClient = mUfsManager.get(createUfsBlockOptions.getMountId());
      alluxio.resource.CloseableResource<UnderFileSystem> ufsResource =
          ufsClient.acquireUfsResource();
      context.setUfsResource(ufsResource);
      String ufsString = MetricsSystem.escape(ufsClient.getUfsMountPointUri());
      String ufsPath = BlockUtils.getUfsBlockPath(ufsClient, request.getId());
      UnderFileSystem ufs = ufsResource.get();
      // Set the atomic flag to be true to ensure only the creation of this file is atomic on close.
      OutputStream ufsOutputStream =
          ufs.create(ufsPath, CreateOptions.defaults().setEnsureAtomic(true).setCreateParent(true));
      context.setOutputStream(ufsOutputStream);
      context.setUfsPath(ufsPath);

      String counterName = Metric.getMetricNameWithTags(WorkerMetrics.BYTES_WRITTEN_UFS,
          WorkerMetrics.TAG_UFS, ufsString);
      String meterName = Metric.getMetricNameWithTags(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT,
          WorkerMetrics.TAG_UFS, ufsString);
      context.setCounter(MetricsSystem.counter(counterName));
      context.setMeter(MetricsSystem.meter(meterName));
    }

    /**
     * Transfers data from block store to UFS.
     *
     * @param context context of this request
     * @param pos number of bytes in block store to write in the UFS block
     */
    private void transferToUfsBlock(BlockWriteRequestContext context, long pos) throws Exception {
      OutputStream ufsOutputStream = context.getOutputStream();

      long sessionId = context.getRequest().getSessionId();
      long blockId = context.getRequest().getId();
      TempBlockMeta block = mWorker.getBlockStore().getTempBlockMeta(sessionId, blockId);
      if (block == null) {
        throw new NotFoundException("block " + blockId + " not found");
      }
      FileRegion fileRegion = new DefaultFileRegion(new File(block.getPath()), 0, pos);
      fileRegion.transferTo(Channels.newChannel(ufsOutputStream), 0);
      fileRegion.release();
    }
  }
}
