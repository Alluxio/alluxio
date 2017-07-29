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
@NotThreadSafe
final class UfsFileWriteHandler extends AbstractWriteHandler {
  private final UfsManager mUfsManager;

  /**
   * The UFS File write request internal representation.
   */
  @NotThreadSafe
  static final class UfsFileWriteRequest extends BaseWriteRequest {
    private UnderFileSystem mUnderFileSystem;
    private OutputStream mOutputStream;
    private final String mUfsPath;
    private final Protocol.CreateUfsFileOptions mCreateUfsFileOptions;

    UfsFileWriteRequest(Protocol.WriteRequest request)
        throws Exception {
      super(request.getId());
      mUfsPath = request.getCreateUfsFileOptions().getUfsPath();
      mCreateUfsFileOptions = request.getCreateUfsFileOptions();
    }

    /**
     * @return the output stream
     */
    public OutputStream getOutputStream() {
      return mOutputStream;
    }

    /**
     * @return the UFS path
     */
    public String getUfsPath() {
      return mUfsPath;
    }

    /**
     * @return the handler of the UFS
     */
    @Nullable
    public UnderFileSystem getUnderFileSystem() {
      return mUnderFileSystem;
    }

    /**
     * @return the options to create UFS file
     */
    public Protocol.CreateUfsFileOptions getCreateUfsFileOptions() {
      return mCreateUfsFileOptions;
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
  protected BaseWriteRequest createWriteRequest(RPCProtoMessage msg) throws Exception {
    return new UfsFileWriteRequest(msg.getMessage().asWriteRequest());
  }

  @Override
  protected void completeWriteRequest(Channel channel) throws Exception {
    UfsFileWriteRequest request = (UfsFileWriteRequest) getRequest();
    Preconditions.checkState(request != null);

    if (request.getOutputStream() == null) {
      createUfsFile(channel);
    }
    if (request.getOutputStream() != null) {
      request.getOutputStream().close();
      request.setOutputStream(null);
    }
  }

  @Override
  protected void cancelWriteRequest() throws Exception {
    UfsFileWriteRequest request = (UfsFileWriteRequest) getRequest();
    Preconditions.checkState(request != null);

    // TODO(calvin): Consider adding cancel to the ufs stream api.
    if (request.getOutputStream() != null && request.getUnderFileSystem() != null) {
      request.getOutputStream().close();
      request.getUnderFileSystem().deleteFile(request.getUfsPath());
      request.setOutputStream(null);
    }
  }

  @Override
  protected void cleanupWriteRequest() throws Exception {
    cancelWriteRequest();
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

  private void createUfsFile(Channel channel) throws IOException {
    UfsFileWriteRequest request = (UfsFileWriteRequest) getRequest();
    Preconditions.checkState(request != null);
    Protocol.CreateUfsFileOptions createUfsFileOptions = request.getCreateUfsFileOptions();
    UfsInfo ufsInfo = mUfsManager.get(createUfsFileOptions.getMountId());
    UnderFileSystem ufs = ufsInfo.getUfs();
    request.setUnderFileSystem(ufs);
    request.setOutputStream(ufs.create(request.getUfsPath(),
        CreateOptions.defaults().setOwner(createUfsFileOptions.getOwner())
            .setGroup(createUfsFileOptions.getGroup())
            .setMode(new Mode((short) createUfsFileOptions.getMode()))));
    String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
    String metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
    request.setCounter(MetricsSystem.workerCounter(metricName));
  }
}
