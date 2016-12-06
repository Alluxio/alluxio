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

package alluxio.client.block.stream;

import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class UnderFileSystemFileInStream extends PacketInStream {
  private WorkerNetAddress mWorkerNetAddress;

  public UnderFileSystemFileInStream(WorkerNetAddress workerNetAddress, long ufsFileId)
      throws IOException {
    super(ufsFileId, Long.MAX_VALUE);
    mWorkerNetAddress = workerNetAddress;
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    closePacketReader();
    mClosed = true;
  }

  protected PacketReader createPacketReader(long offset, long len) throws IOException {
    return NettyPacketReader
        .createFilePacketReader(NetworkAddressUtils.getDataPortSocketAddress(mWorkerNetAddress),
            mId, offset, (int) len);
  }
}
