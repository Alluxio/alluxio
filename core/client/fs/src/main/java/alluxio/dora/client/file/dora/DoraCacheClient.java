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

package alluxio.dora.client.file.dora;

import static com.google.common.base.Preconditions.checkState;

import alluxio.dora.client.block.BlockWorkerInfo;
import alluxio.dora.client.block.stream.BlockWorkerClient;
import alluxio.dora.client.block.stream.DataReader;
import alluxio.dora.client.block.stream.GrpcDataReader;
import alluxio.dora.client.block.stream.NettyDataReader;
import alluxio.dora.client.file.FileSystemContext;
import alluxio.dora.client.file.URIStatus;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.grpc.FileInfo;
import alluxio.dora.grpc.GetStatusPOptions;
import alluxio.dora.grpc.GetStatusPRequest;
import alluxio.dora.grpc.GrpcUtils;
import alluxio.dora.grpc.ListStatusPOptions;
import alluxio.dora.grpc.ListStatusPRequest;
import alluxio.dora.grpc.ReadRequest;
import alluxio.dora.proto.dataserver.Protocol;
import alluxio.dora.resource.CloseableResource;
import alluxio.dora.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Dora cache client.
 */
public class DoraCacheClient {
  public static final int DUMMY_BLOCK_ID = -1;
  public static final int PREFERRED_WORKER_COUNT = 1;
  private final FileSystemContext mContext;
  private final long mChunkSize;
  private final WorkerLocationPolicy mWorkerLocationPolicy;

  private final boolean mNettyTransEnabled;

  /**
   * Constructor.
   *
   * @param context
   * @param workerLocationPolicy
   */
  public DoraCacheClient(FileSystemContext context, WorkerLocationPolicy workerLocationPolicy) {
    mContext = context;
    mWorkerLocationPolicy = workerLocationPolicy;
    mChunkSize = mContext.getClusterConf().getBytes(
        PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES);
    mNettyTransEnabled =
        context.getClusterConf().getBoolean(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED);
  }

  /**
   * Get a stream to read the data from dora cache cluster.
   *
   * @param status
   * @param ufsOptions
   * @return the input stream
   */
  public DoraCacheFileInStream getInStream(URIStatus status,
                                           Protocol.OpenUfsBlockOptions ufsOptions) {
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(status.getPath());
    // Construct the partial read request
    DataReader.Factory readerFactory;
    if (mNettyTransEnabled) {
      readerFactory = createNettyDataReader(workerNetAddress, ufsOptions);
    } else {
      readerFactory = createGrpcDataReader(workerNetAddress, ufsOptions);
    }
    return new DoraCacheFileInStream(readerFactory, status.getLength());
  }

  private GrpcDataReader.Factory createGrpcDataReader(
      WorkerNetAddress workerNetAddress,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    ReadRequest.Builder builder = ReadRequest.newBuilder()
        .setBlockId(DUMMY_BLOCK_ID)
        .setOpenUfsBlockOptions(ufsOptions)
        .setChunkSize(mChunkSize);
    return new GrpcDataReader.Factory(mContext, workerNetAddress, builder);
  }

  private NettyDataReader.Factory createNettyDataReader(
      WorkerNetAddress workerNetAddress,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    Protocol.ReadRequest.Builder builder = Protocol.ReadRequest.newBuilder()
        .setBlockId(DUMMY_BLOCK_ID)
        .setOpenUfsBlockOptions(ufsOptions)
        .setChunkSize(mChunkSize);
    return new NettyDataReader.Factory(mContext, workerNetAddress, builder);
  }

  /**
   * List Status from Worker.
   * @param path
   * @param options
   * @return list of URIStatus
   * @throws RuntimeException
   */
  public List<URIStatus> listStatus(String path, ListStatusPOptions options)
      throws RuntimeException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      List<URIStatus> result = new ArrayList<>();
      client.get().listStatus(ListStatusPRequest.newBuilder().setPath(path)
              .setOptions(options).build())
          .forEachRemaining(
              (pListStatusResponse) -> result.addAll(pListStatusResponse.getFileInfosList().stream()
                  .map((pFileInfo) -> new URIStatus(GrpcUtils.fromProto(pFileInfo)))
                  .collect(Collectors.toList())));
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get status.
   *
   * @param path
   * @param options
   * @return uri status
   */
  public URIStatus getStatus(String path, GetStatusPOptions options) {
    return getStatusByGrpc(path, options);
  }

  private URIStatus getStatusByGrpc(String path, GetStatusPOptions options) {
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
