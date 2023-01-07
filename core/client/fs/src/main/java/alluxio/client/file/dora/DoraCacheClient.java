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

package alluxio.client.file.dora;

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.block.stream.GrpcDataReader;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.FileInfo;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ReadRequest;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

/**
 * Dora cache client.
 */
public class DoraCacheClient {
  public static final int DUMMY_BLOCK_ID = -1;
  public static final int PREFERRED_WORKER_COUNT = 1;
  private final FileSystemContext mContext;
  private final long mChunkSize;
  private final WorkerLocationPolicy mWorkerLocationPolicy;

  /**
   * Constructor.
   * @param context
   * @param workerLocationPolicy
   */
  public DoraCacheClient(FileSystemContext context, WorkerLocationPolicy workerLocationPolicy) {
    mContext = context;
    mWorkerLocationPolicy = workerLocationPolicy;
    mChunkSize = mContext.getClusterConf().getBytes(
        PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES);
  }

  /**
   * Get a stream to read the data from dora cache cluster.
   * @param status
   * @param ufsOptions
   * @return the input stream
   */
  public DoraCacheFileInStream getInStream(URIStatus status,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(status.getPath());
    // Construct the partial read request
    ReadRequest.Builder builder = ReadRequest.newBuilder()
        .setBlockId(DUMMY_BLOCK_ID)
        .setOpenUfsBlockOptions(ufsOptions)
        .setChunkSize(mChunkSize);
    GrpcDataReader.Factory grpcReaderFactory =
        new GrpcDataReader.Factory(mContext, workerNetAddress, builder);
    return new DoraCacheFileInStream(grpcReaderFactory, status.getLength());
  }

  /**
   * Get status.
   * @param path
   * @param options
   * @return uri status
   */
  public URIStatus getStatus(String path, GetStatusPOptions options) {
    try (CloseableResource<BlockWorkerClient> client =
          mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      GetStatusPRequest request = GetStatusPRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      FileInfo fileInfo = client.get().getStatus(request).getFileInfo();
      return new URIStatus(GrpcUtils.fromProto(fileInfo));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private WorkerNetAddress getWorkerNetAddress(String path) {
    List<BlockWorkerInfo> workers = null;
    try {
      workers = mContext.getCachedWorkers();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<BlockWorkerInfo> preferredWorkers =
        mWorkerLocationPolicy.getPreferredWorkers(workers,
            path, PREFERRED_WORKER_COUNT);
    checkState(preferredWorkers.size() > 0);
    WorkerNetAddress workerNetAddress = preferredWorkers.get(0).getNetAddress();
    return workerNetAddress;
  }
}
