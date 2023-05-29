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

import alluxio.AlluxioURI;
import alluxio.CloseableSupplier;
import alluxio.PositionReader;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.block.stream.GrpcDataReader;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.PositionReadFileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.dora.netty.NettyDataReader;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.ufs.DoraOutStream;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.FileInfo;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ReadRequest;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;

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
  public PositionReadFileInStream getInStream(URIStatus status,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(status.getPath());
    // Construct the partial read request
    NettyDataReader reader;
    if (mNettyTransEnabled) {
      reader = createNettyDataReader(workerNetAddress, ufsOptions);
    } else {
      throw new UnsupportedOperationException("Grpc dora reader not implemented");
    }
    return new PositionReadFileInStream(reader, status.getLength());
  }

  /**
   * Create a OutStream to write data to dora cluster.
   * @param status
   * @param outStreamOptions
   * @param fsContext
   * @return the out stream
   * @throws IOException
   */
  public DoraOutStream getOutStream(URIStatus status,
                                    OutStreamOptions outStreamOptions,
                                    FileSystemContext fsContext) throws IOException {
    return new DoraOutStream(new AlluxioURI(status.getUfsPath()),
                                    outStreamOptions,
                                    fsContext,
                                    this);
  }

  protected long getChunkSize() {
    return mChunkSize;
  }

  /**
   * @param status
   * @param ufsOptions
   * @param externalPositionReader
   * @return a netty position reader
   */
  public DoraCachePositionReader createNettyPositionReader(URIStatus status,
      Protocol.OpenUfsBlockOptions ufsOptions,
      CloseableSupplier<PositionReader> externalPositionReader) {
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(status.getPath());
    // Construct the partial read request
    NettyDataReader reader = createNettyDataReader(workerNetAddress, ufsOptions);
    return new DoraCachePositionReader(reader, status.getLength(), externalPositionReader);
  }

  protected GrpcDataReader.Factory createGrpcDataReader(
      WorkerNetAddress workerNetAddress,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    ReadRequest.Builder builder = ReadRequest.newBuilder()
        .setBlockId(DUMMY_BLOCK_ID)
        .setOpenUfsBlockOptions(ufsOptions)
        .setChunkSize(mChunkSize);
    return new GrpcDataReader.Factory(mContext, workerNetAddress, builder);
  }

  protected NettyDataReader createNettyDataReader(
      WorkerNetAddress workerNetAddress,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    Protocol.ReadRequest.Builder builder = Protocol.ReadRequest.newBuilder()
        .setBlockId(DUMMY_BLOCK_ID)
        .setOpenUfsBlockOptions(ufsOptions)
        .setChunkSize(mChunkSize);
    return new NettyDataReader(mContext, workerNetAddress, builder);
  }

  /**
   * List Status from Worker.
   * @param path
   * @param options
   * @return list of URIStatus
   * @throws RuntimeException
   */
  public List<URIStatus> listStatus(String path, ListStatusPOptions options)
      throws PermissionDeniedException {
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
  public URIStatus getStatus(String path, GetStatusPOptions options)
      throws PermissionDeniedException {
    return getStatusByGrpc(path, options);
  }

  protected URIStatus getStatusByGrpc(String path, GetStatusPOptions options)
      throws PermissionDeniedException {
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

  /**
   * Create File.
   * @param path the file path
   * @param options the option for creating operation
   * @return URIStatus of new file
   * @throws RuntimeException
   */
  public URIStatus createFile(String path, CreateFilePOptions options) {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      CreateFilePRequest request = CreateFilePRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      FileInfo fileInfo = client.get().createFile(request).getFileInfo();
      return new URIStatus(GrpcUtils.fromProto(fileInfo));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Mark the newly created and written file as complete.
   *
   * This is called when out stream is closed. This is equivalent to close() in some file system.
   * @param path The file path
   * @param options the close option
   */
  public void completeFile(String path, CompleteFilePOptions options) {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      CompleteFilePRequest request = CompleteFilePRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      client.get().completeFile(request);
      return;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the worker net address of the specific file path.
   * @param path the file path
   * @return the related worker net address where file locates
   */
  public WorkerNetAddress getWorkerNetAddress(String path) {
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
