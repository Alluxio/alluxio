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
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MetricInfo;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
@alluxio.annotation.SuppressFBWarnings(
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
   * @param ufsManager the file data manager
   * @param userInfo the authenticated user info
   */
  UfsFileWriteHandler(UfsManager ufsManager, StreamObserver<WriteResponse> responseObserver,
      AuthenticatedUserInfo userInfo) {
    super(responseObserver, userInfo);
    mUfsManager = ufsManager;
  }

  @Override
  protected UfsFileWriteRequestContext createRequestContext(WriteRequest msg) {
    return new UfsFileWriteRequestContext(msg);
  }

  @Override
  protected void completeRequest(UfsFileWriteRequestContext context)
      throws Exception {
    if (context == null) {
      return;
    }
    if (context.getOutputStream() == null) {
      createUfsFile(context);
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
      context.getUfsResource().get().deleteExistingFile(request.getUfsPath());
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
  protected void flushRequest(UfsFileWriteRequestContext context)
      throws Exception {
    Preconditions.checkState(context != null);
    if (context.getOutputStream() != null) {
      context.getOutputStream().flush();
    }
  }

  @Override
  protected void writeBuf(UfsFileWriteRequestContext context,
      StreamObserver<WriteResponse> observer, DataBuffer buf, long pos) throws Exception {
    Preconditions.checkState(context != null);
    if (context.getOutputStream() == null) {
      createUfsFile(context);
    }
    buf.readBytes(context.getOutputStream(), buf.readableBytes());
  }

  @Override
  protected String getLocationInternal(UfsFileWriteRequestContext context) {
    return context.getRequest().getCreateUfsFileOptions().getUfsPath();
  }

  private void createUfsFile(UfsFileWriteRequestContext context)
      throws IOException {
    UfsFileWriteRequest request = context.getRequest();
    Preconditions.checkState(request != null);
    Protocol.CreateUfsFileOptions createUfsFileOptions = request.getCreateUfsFileOptions();
    UfsManager.UfsClient ufsClient = mUfsManager.get(createUfsFileOptions.getMountId());
    CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource();
    context.setUfsResource(ufsResource);
    UnderFileSystem ufs = ufsResource.get();
    CreateOptions createOptions = CreateOptions.defaults(ServerConfiguration.global())
        .setCreateParent(true)
        .setOwner(createUfsFileOptions.getOwner()).setGroup(createUfsFileOptions.getGroup())
        .setMode(new Mode((short) createUfsFileOptions.getMode()));
    if (createUfsFileOptions.hasAcl()) {
      // This acl information will be ignored by all but HDFS implementations
      createOptions.setAcl(ProtoUtils.fromProto(createUfsFileOptions.getAcl()));
    }
    context.setOutputStream(ufs.createNonexistingFile(request.getUfsPath(), createOptions));
    context.setCreateOptions(createOptions);
    String ufsString = MetricsSystem.escape(ufsClient.getUfsMountPointUri());

    MetricKey counterKey = MetricKey.WORKER_BYTES_WRITTEN_UFS;
    MetricKey meterKey = MetricKey.WORKER_BYTES_WRITTEN_UFS_THROUGHPUT;
    context.setCounter(MetricsSystem.counterWithTags(counterKey.getName(),
        counterKey.isClusterAggregated(), MetricInfo.TAG_UFS, ufsString));
    context.setMeter(MetricsSystem.meterWithTags(meterKey.getName(),
        meterKey.isClusterAggregated(), MetricInfo.TAG_UFS, ufsString));
  }
}
