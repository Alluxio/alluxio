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
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
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
final class UfsFileWriteHandler
    extends AbstractWriteHandler<UfsFileWriteHandler.UfsFileWriteRequest> {
  private final UfsManager mUfsManager;

  /**
   * The UFS File write request internal representation.
   */
  @NotThreadSafe
  static final class UfsFileWriteRequest extends WriteRequest {
    private final String mUfsPath;
    private final Protocol.CreateUfsFileOptions mCreateUfsFileOptions;
    private final Context mContext = new Context();

    UfsFileWriteRequest(Protocol.WriteRequest request) {
      super(request.getId());
      mUfsPath = request.getCreateUfsFileOptions().getUfsPath();
      mCreateUfsFileOptions = request.getCreateUfsFileOptions();
    }

    /**
     * @return the UFS path
     */
    public String getUfsPath() {
      return mUfsPath;
    }

    /**
     * @return the options to create UFS file
     */
    public Protocol.CreateUfsFileOptions getCreateUfsFileOptions() {
      return mCreateUfsFileOptions;
    }

    /**
     * @return the context of this request
     */
    public Context getContext() {
      return mContext;
    }

    /**
     * The context of this request, including some runtime state to handle this request.
     */
    @NotThreadSafe
    final class Context {
      private UnderFileSystem mUnderFileSystem;
      private OutputStream mOutputStream;
      private Counter mCounter;

      Context() {}

      /**
       * @return the output stream
       */
      @Nullable
      public OutputStream getOutputStream() {
        return mOutputStream;
      }

      /**
       * @return the handler of the UFS
       */
      @Nullable
      public UnderFileSystem getUnderFileSystem() {
        return mUnderFileSystem;
      }

      /**
       * @return the counter
       */
      @Nullable
      public Counter getCounter() {
        return mCounter;
      }

      /**
       * Sets the output stream.
       *
       * @param outputStream output stream to set
       */
      public void setOutputStream(OutputStream outputStream) {
        mOutputStream = outputStream;
      }

      /**
       * Sets the handler of the UFS.
       *
       * @param underFileSystem UFS to set
       */
      public void setUnderFileSystem(UnderFileSystem underFileSystem) {
        mUnderFileSystem = underFileSystem;
      }

      /**
       * Sets the counter.
       *
       * @param counter counter to set
       */
      public void setCounter(Counter counter) {
        mCounter = counter;
      }
    }
  }

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
  protected UfsFileWriteRequest createRequest(RPCProtoMessage msg) throws Exception {
    return new UfsFileWriteRequest(msg.getMessage().asWriteRequest());
  }

  @Override
  protected void completeRequest(Channel channel) throws Exception {
    UfsFileWriteRequest request = getRequest();
    if (request == null) {
      return;
    }
    if (request.getContext().getOutputStream() == null) {
      createUfsFile(channel);
    }
    if (request.getContext().getOutputStream() != null) {
      request.getContext().getOutputStream().close();
      request.getContext().setOutputStream(null);
    }
  }

  @Override
  protected void cancelRequest() throws Exception {
    UfsFileWriteRequest request = getRequest();
    if (request == null) {
      return;
    }
    // TODO(calvin): Consider adding cancel to the ufs stream api.
    if (request.getContext().getOutputStream() != null
        && request.getContext().getUnderFileSystem() != null) {
      request.getContext().getOutputStream().close();
      request.getContext().getUnderFileSystem().deleteFile(request.getUfsPath());
      request.getContext().setOutputStream(null);
    }
  }

  @Override
  protected void cleanupRequest() throws Exception {
    cancelRequest();
  }

  @Override
  protected void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
    UfsFileWriteRequest request = getRequest();
    Preconditions.checkState(request != null);
    if (request.getContext().getOutputStream() == null) {
      createUfsFile(channel);
    }

    buf.readBytes(getRequest().getContext().getOutputStream(), buf.readableBytes());
  }

  private void createUfsFile(Channel channel) throws IOException {
    UfsFileWriteRequest request = getRequest();
    Preconditions.checkState(request != null);
    Protocol.CreateUfsFileOptions createUfsFileOptions = request.getCreateUfsFileOptions();
    UfsInfo ufsInfo = mUfsManager.get(createUfsFileOptions.getMountId());
    UnderFileSystem ufs = ufsInfo.getUfs();
    request.getContext().setUnderFileSystem(ufs);
    request.getContext().setOutputStream(ufs.create(request.getUfsPath(),
        CreateOptions.defaults().setOwner(createUfsFileOptions.getOwner())
            .setGroup(createUfsFileOptions.getGroup())
            .setMode(new Mode((short) createUfsFileOptions.getMode()))));
    String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
    String metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
    request.getContext().setCounter(MetricsSystem.workerCounter(metricName));
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    UfsFileWriteRequest request = getRequest();
    Preconditions.checkState(request != null);
    Counter counter = request.getContext().getCounter();
    Preconditions.checkState(counter != null);
    counter.inc(bytesWritten);
  }
}
