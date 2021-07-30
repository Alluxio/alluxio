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

package alluxio.stress;

import alluxio.conf.PropertyKey;
import alluxio.grpc.FileSystemMasterWorkerServiceGrpc;
import alluxio.grpc.GetPinnedFileIdsPRequest;
import alluxio.master.MasterClientContext;
import alluxio.worker.file.FileSystemMasterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Helper client that is used to minimizes the deserialization overhead by calling
 * getPinnedFileIdsCount instead of getPinnedFileIdsList
 */
public class PinListFileSystemMasterClient extends FileSystemMasterClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(PinListFileSystemMasterClient.class);
  private FileSystemMasterWorkerServiceGrpc.FileSystemMasterWorkerServiceBlockingStub mClient =
      null;

  public PinListFileSystemMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = FileSystemMasterWorkerServiceGrpc.newBlockingStub(mChannel);
  }

  public int getPinListLength() throws IOException {
    return retryRPC(() -> mClient.withDeadlineAfter(mContext.getClusterConf()
            .getMs(PropertyKey.WORKER_MASTER_PERIODICAL_RPC_TIMEOUT), TimeUnit.MILLISECONDS)
            .getPinnedFileIds(GetPinnedFileIdsPRequest.newBuilder().build())
            .getPinnedFileIdsCount(),
        LOG, "GetPinList", "");
  }
}
