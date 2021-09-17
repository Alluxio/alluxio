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

package alluxio.worker.grpc;

import alluxio.conf.ServerConfiguration;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.WriteRequestCommand;
import alluxio.grpc.WriteResponse;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.worker.BlockUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles UFS block write request. Instead of writing a block to tiered storage, this
 * handler writes the block into UFS .
 */
@alluxio.annotation.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class UfsFallbackBlockWriteHandler
    extends AbstractWriteHandler<BlockWriteRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(UfsFallbackBlockWriteHandler.class);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  private final UfsManager mUfsManager;
  private final BlockWriteHandler mBlockWriteHandler;
  private final boolean mDomainSocketEnabled;

  /**
   * Creates an instance of {@link UfsFallbackBlockWriteHandler}.
   *
   * @param blockWorker the block worker
   * @param userInfo the authenticated user info
   * @param domainSocketEnabled whether using a domain socket
   */
  UfsFallbackBlockWriteHandler(BlockWorker blockWorker, UfsManager ufsManager,
      StreamObserver<WriteResponse> responseObserver, AuthenticatedUserInfo userInfo,
      boolean domainSocketEnabled) {
    super(responseObserver, userInfo);
    mWorker = blockWorker;
    mUfsManager = ufsManager;
    mBlockWriteHandler =
        new BlockWriteHandler(blockWorker, responseObserver, userInfo, domainSocketEnabled);
    mDomainSocketEnabled = domainSocketEnabled;
  }

  @Override
  protected BlockWriteRequestContext createRequestContext(alluxio.grpc.WriteRequest msg)
      throws Exception {
    BlockWriteRequestContext context = new BlockWriteRequestContext(msg, FILE_BUFFER_SIZE);
    if (mDomainSocketEnabled) {
      context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_WRITTEN_DOMAIN.getName()));
      context.setMeter(MetricsSystem
          .meter(MetricKey.WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName()));
    } else {
      context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_WRITTEN_REMOTE.getName()));
      context.setMeter(MetricsSystem
          .meter(MetricKey.WORKER_BYTES_WRITTEN_REMOTE_THROUGHPUT.getName()));
    }
    BlockWriteRequest request = context.getRequest();
    Preconditions.checkState(request.hasCreateUfsBlockOptions());
    // if it is already a UFS fallback from short-circuit write, avoid writing to local again
    context.setWritingToLocal(!request.getCreateUfsBlockOptions().getFallback());
    if (context.isWritingToLocal()) {
      mWorker.createBlock(request.getSessionId(), request.getId(), request.getTier(),
          request.getMediumType(), FILE_BUFFER_SIZE);
    }
    return context;
  }

  @Override
  protected void completeRequest(BlockWriteRequestContext context)
      throws Exception {
    if (context.isWritingToLocal()) {
      mBlockWriteHandler.completeRequest(context);
    } else {
      mWorker.commitBlockInUfs(context.getRequest().getId(), context.getPos());
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
      mBlockWriteHandler.cancelRequest(context);
    } else {
      if (context.getOutputStream() != null) {
        context.getOutputStream().close();
        context.setOutputStream(null);
      }
      if (context.getUfsResource() != null) {
        context.getUfsResource().get().deleteExistingFile(context.getUfsPath());
      }
    }
    if (context.getUfsResource() != null) {
      context.getUfsResource().close();
    }
  }

  @Override
  protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
    if (context.isWritingToLocal()) {
      mBlockWriteHandler.cleanupRequest(context);
    } else {
      cancelRequest(context);
    }
  }

  @Override
  protected void flushRequest(BlockWriteRequestContext context) throws Exception {
    if (context.isWritingToLocal()) {
      mBlockWriteHandler.flushRequest(context);
    } else if (context.getOutputStream() != null) {
      context.getOutputStream().flush();
    }
  }

  @Override
  protected void writeBuf(BlockWriteRequestContext context,
      StreamObserver<WriteResponse> responseObserver, DataBuffer buf, long pos) throws Exception {
    if (context.isWritingToLocal()) {
      // TODO(binfan): change signature of writeBuf to pass current offset and length of buffer.
      // Currently pos is the calculated offset after writeBuf succeeds.
      long posBeforeWrite = pos - buf.readableBytes();
      try {
        mBlockWriteHandler.writeBuf(context, responseObserver, buf, pos);
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
      createUfsBlock(context);
      if (posBeforeWrite > 0) {
        transferToUfsBlock(context, posBeforeWrite);
      }
      // close the original block writer and remove the temp file
      mBlockWriteHandler.cancelRequest(context);
    }
    if (context.getOutputStream() == null) {
      createUfsBlock(context);
    }
    buf.readBytes(context.getOutputStream(), buf.readableBytes());
  }

  @Override
  protected void handleCommand(WriteRequestCommand command, BlockWriteRequestContext context)
      throws Exception {
    if (command.hasCreateUfsBlockOptions()
        && command.getOffset() == 0
        && command.getCreateUfsBlockOptions().hasBytesInBlockStore()) {
      long ufsFallbackInitBytes = command.getCreateUfsBlockOptions().getBytesInBlockStore();
      context.setPos(context.getPos() + ufsFallbackInitBytes);
      initUfsFallback(context);
    }
  }

  @Override
  protected String getLocationInternal(BlockWriteRequestContext context) {
    Protocol.CreateUfsBlockOptions createUfsBlockOptions =
        context.getRequest().getCreateUfsBlockOptions();

    if (createUfsBlockOptions == null) {
      return String.format("blockId-%d", context.getRequest().getId());
    }

    UfsManager.UfsClient ufsClient;
    try {
      ufsClient = mUfsManager.get(createUfsBlockOptions.getMountId());
    } catch (Throwable e) {
      return String.format("blockId-%d", context.getRequest().getId());
    }

    return BlockUtils.getUfsBlockPath(ufsClient, context.getRequest().getId());
  }

  private void initUfsFallback(BlockWriteRequestContext context) throws Exception {
    Preconditions.checkState(!context.isWritingToLocal());
    if (context.getOutputStream() == null) {
      createUfsBlock(context);
    }
    // transfer data from the temp block to UFS
    transferToUfsBlock(context, context.getPos());
  }

  /**
   * Creates a UFS block and initialize it with bytes read from block store.
   *
   * @param context context of this request
   */
  private void createUfsBlock(BlockWriteRequestContext context)
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
        ufs.createNonexistingFile(ufsPath,
            CreateOptions.defaults(ServerConfiguration.global()).setEnsureAtomic(true)
                .setCreateParent(true));
    context.setOutputStream(ufsOutputStream);
    context.setUfsPath(ufsPath);

    MetricKey counterKey = MetricKey.WORKER_BYTES_WRITTEN_UFS;
    MetricKey meterKey = MetricKey.WORKER_BYTES_WRITTEN_UFS_THROUGHPUT;
    context.setCounter(MetricsSystem.counterWithTags(counterKey.getName(),
        counterKey.isClusterAggregated(), MetricInfo.TAG_UFS, ufsString));
    context.setMeter(MetricsSystem.meterWithTags(meterKey.getName(),
        meterKey.isClusterAggregated(), MetricInfo.TAG_UFS, ufsString));
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
    TempBlockMeta block = mWorker.getTempBlockMeta(sessionId, blockId);
    if (block == null) {
      throw new NotFoundException("block " + blockId + " not found");
    }
    Preconditions.checkState(Files.copy(Paths.get(block.getPath()), ufsOutputStream) == pos);
  }
}
