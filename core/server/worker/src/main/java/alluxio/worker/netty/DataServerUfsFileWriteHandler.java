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
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles writes to a file in the under file system. Due to the semantics enforced
 * on under file systems, the client must write all the data of a file through the same stream to
 * the under file system. This prevents us from handling the writes at a block level.
 *
 * For more information about the implementation of read/write buffering, see
 * {@link DataServerWriteHandler}.
 *
 * For more information about the implementation of the client side writer, see
 * UnderFileSystemFileOutStream.
 */
@NotThreadSafe
final class DataServerUfsFileWriteHandler extends DataServerWriteHandler {
  private final UfsManager mUfsManager;

  private class FileWriteRequestInternal extends WriteRequestInternal {
    private final String mUfsPath;
    private final Protocol.WriteRequest mWriteRequest;

    private UnderFileSystem mUnderFileSystem;
    private OutputStream mOutputStream;
    private Counter mCounter;

    FileWriteRequestInternal(Protocol.WriteRequest request) throws Exception {
      super(request.getId());
      mWriteRequest = request;
      mUfsPath = request.getCreateUfsFileOptions().getUfsPath();
    }

    @Override
    public void close(Channel channel) throws IOException {
      if (mOutputStream == null) {
        createUfsFile(channel);
      }
      if (mOutputStream != null) {
        mOutputStream.close();
        mOutputStream = null;
      }
    }

    @Override
    void cancel() throws IOException {
      // TODO(calvin): Consider adding cancel to the ufs stream api.
      if (mOutputStream != null && mUnderFileSystem != null) {
        mOutputStream.close();
        mUnderFileSystem.deleteFile(mUfsPath);
        mOutputStream = null;
      }
    }

    @Override
    void cleanup() throws IOException {
      cancel();
    }
  }

  /**
   * Creates an instance of {@link DataServerUfsFileWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param ufsManager the file data manager
   */
  DataServerUfsFileWriteHandler(ExecutorService executorService, UfsManager ufsManager) {
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

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   */
  @Override
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    super.initializeRequest(msg);
    if (mRequest == null) {
      mRequest = new FileWriteRequestInternal(msg.getMessage().asWriteRequest());
    }
  }

  @Override
  protected void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
    FileWriteRequestInternal request = (FileWriteRequestInternal) mRequest;
    if (request.mOutputStream == null) {
      createUfsFile(channel);
    }

    buf.readBytes(((FileWriteRequestInternal) mRequest).mOutputStream, buf.readableBytes());
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    ((FileWriteRequestInternal) mRequest).mCounter.inc(bytesWritten);
  }

  private void createUfsFile(Channel channel) throws IOException {
    FileWriteRequestInternal request = (FileWriteRequestInternal) mRequest;
    Protocol.CreateUfsFileOptions createUfsFileOptions =
        request.mWriteRequest.getCreateUfsFileOptions();
    UfsInfo ufsInfo = mUfsManager.get(createUfsFileOptions.getMountId());
    request.mUnderFileSystem = ufsInfo.getUfs();
    request.mOutputStream = request.mUnderFileSystem.create(request.mUfsPath,
        CreateOptions.defaults().setOwner(createUfsFileOptions.getOwner())
            .setGroup(createUfsFileOptions.getGroup())
            .setMode(new Mode((short) createUfsFileOptions.getMode())));
    String ufsString = MetricsSystem.escape(ufsInfo.getUfsMountPointUri());
    String metricName = String.format("BytesWrittenUfs-Ufs:%s", ufsString);
    request.mCounter = MetricsSystem.workerCounter(metricName);
  }
}
