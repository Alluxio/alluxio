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
import alluxio.client.file.DoraFileOutStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.PositionReadFileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.dora.netty.NettyDataReader;
import alluxio.client.file.dora.netty.NettyDataWriter;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.grpc.CacheDataRequest;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.CreateFilePResponse;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.ExistsPRequest;
import alluxio.grpc.ExistsPResponse;
import alluxio.grpc.FileInfo;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RequestType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.SetAttributePRequest;
import alluxio.membership.WorkerClusterView;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Dora cache client.
 */
public class DoraCacheClient {
  public static final int DUMMY_BLOCK_ID = -1;
  private final FileSystemContext mContext;
  private final long mChunkSize;
  private final WorkerLocationPolicy mWorkerLocationPolicy;

  private final boolean mNettyTransEnabled;

  private final int mPreferredWorkerCount;

  private final boolean mEnableDynamicHashRing;
  private static final Logger LOG = LoggerFactory.getLogger(DoraCacheClient.class);

  /**
   * Constructor.
   *
   * @param context
   */
  public DoraCacheClient(FileSystemContext context) {
    mContext = context;
    mWorkerLocationPolicy = WorkerLocationPolicy.Factory.create(context.getClusterConf());
    mChunkSize = mContext.getClusterConf().getBytes(
        PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES);
    mNettyTransEnabled =
        context.getClusterConf().getBoolean(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED);
    mEnableDynamicHashRing =
        context.getClusterConf().getBoolean(PropertyKey.USER_DYNAMIC_CONSISTENT_HASH_RING_ENABLED);
    int minReplicaCount = context.getClusterConf().getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
    mPreferredWorkerCount = Math.max(1, minReplicaCount);
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
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(status.getUfsPath());
    // Construct the partial read request
    NettyDataReader reader;
    if (mNettyTransEnabled) {
      reader = createNettyDataReader(workerNetAddress, ufsOptions);
    } else {
      throw new UnsupportedOperationException("Grpc dora reader not implemented");
    }
    return new PositionReadFileInStream(reader, status, this);
  }

  /**
   * Get a stream to write the data to dora cache cluster.
   *
   * @param alluxioPath the alluxio path to be written
   * @param fsContext the file system context
   * @param outStreamOptions the output stream options
   * @param ufsOutStream the UfsOutStream for writing data to UFS
   * @param uuid the UUID for a certain FileOutStream
   * @return the output stream
   */
  public DoraFileOutStream getOutStream(AlluxioURI alluxioPath, FileSystemContext fsContext,
      OutStreamOptions outStreamOptions, @Nullable FileOutStream ufsOutStream,
      String uuid) throws IOException {
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(alluxioPath.toString());
    NettyDataWriter writer = NettyDataWriter.create(
        fsContext, workerNetAddress, Long.MAX_VALUE, RequestType.ALLUXIO_BLOCK, outStreamOptions);
    return new DoraFileOutStream(this, writer, alluxioPath,
        outStreamOptions, fsContext, ufsOutStream, uuid);
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
      Optional<CloseableSupplier<PositionReader>> externalPositionReader) {
    WorkerNetAddress workerNetAddress = getWorkerNetAddress(status.toString());
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
  public Pair<URIStatus, String> createFile(String path, CreateFilePOptions options)
      throws PermissionDeniedException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      CreateFilePRequest request = CreateFilePRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      CreateFilePResponse response = client.get().createFile(request);
      FileInfo fileInfo = response.getFileInfo();
      String uuid = response.getUuid();
      return new Pair<>(new URIStatus(GrpcUtils.fromProto(fileInfo)), uuid);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get a map that maps file path to the workers list.
   * @param path the file path to check
   * @param options the get status options
   * @return a map that maps file path to the workers list
   * @throws IOException
   */
  public Map<String, List<WorkerNetAddress>> checkFileLocation(String path,
      GetStatusPOptions options) throws IOException {
    Map<String, List<WorkerNetAddress>> pathDistributionMap = new HashMap<>();
    WorkerClusterView workers = mContext.getLiveWorkers();
    for (WorkerInfo worker : workers) {
      try (CloseableResource<BlockWorkerClient> client =
               mContext.acquireBlockWorkerClient(worker.getAddress())) {
        GetStatusPRequest request = GetStatusPRequest.newBuilder()
            .setPath(path)
            .setOptions(options)
            .build();
        try {
          FileInfo fileInfo = client.get().getStatus(request).getFileInfo();
          URIStatus uriStatus = new URIStatus(GrpcUtils.fromProto(fileInfo));
          if (uriStatus.getInAlluxioPercentage() > 0) {
            List<WorkerNetAddress> assignedWorkers = pathDistributionMap.get(path);
            if (assignedWorkers == null) {
              assignedWorkers = new ArrayList<>();
            }
            assignedWorkers.add(worker.getAddress());
            pathDistributionMap.put(path, assignedWorkers);
          }
        } catch (Exception e) {
          // ignore this exception
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return pathDistributionMap;
  }

  /**
   * Mark the newly created and written file as complete.
   *
   * This is called when out stream is closed. This is equivalent to close() in some file system.
   * @param path The file path
   * @param options the close option
   * @param uuid the uuid of its open file handle
   */
  public void completeFile(String path, CompleteFilePOptions options, String uuid)
      throws PermissionDeniedException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      CompleteFilePRequest request = CompleteFilePRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .setUuid(uuid)
          .build();
      client.get().completeFile(request);
      return;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Delete a file.
   * @param path
   * @param options
   */
  public void delete(String path, DeletePOptions options) throws PermissionDeniedException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      DeletePRequest request = DeletePRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      client.get().delete(request);
      return;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Rename a src file/dir to dst file/dir.
   * @param src the source file/dir
   * @param dst the destination file/dir
   * @param options the rename option
   */
  public void rename(String src, String dst, RenamePOptions options)
      throws PermissionDeniedException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(src))) {
      RenamePRequest request = RenamePRequest.newBuilder()
          .setPath(src)
          .setDstPath(dst)
          .setOptions(options)
          .build();
      client.get().rename(request);
      return;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a dir.
   * @param path the name of the dir
   * @param options the option of this operation
   */
  public void createDirectory(String path, CreateDirectoryPOptions options)
      throws PermissionDeniedException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      CreateDirectoryPRequest request = CreateDirectoryPRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      client.get().createDirectory(request);
      return;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Check existence of a file or dir.
   * @param path the path of the file or dir
   * @param options the option of this operation
   * @return true if the file/dir exists, false otherwise
   * @throws InvalidPathException
   * @throws IOException
   * @throws AlluxioException
   */
  public boolean exists(String path, ExistsPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      ExistsPRequest request = ExistsPRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      ExistsPResponse response = client.get().exists(request);
      return response.getExists();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Set attributes for a file or dir.
   * @param path the path of the file or dir
   * @param options the option of this operation
   * @throws FileDoesNotExistException
   * @throws IOException
   * @throws AlluxioException
   */
  public void setAttribute(String path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(path))) {
      SetAttributePRequest request = SetAttributePRequest.newBuilder()
          .setPath(path)
          .setOptions(options)
          .build();
      client.get().setAttribute(request);
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
    try {
      WorkerClusterView workers = mEnableDynamicHashRing ? mContext.getCachedWorkers(
          FileSystemContext.GetWorkerListType.LIVE) : mContext.getCachedWorkers(
          FileSystemContext.GetWorkerListType.ALL);
      checkState(!workers.isEmpty(), "No workers available in the cluster. Lost workers %s",
          mEnableDynamicHashRing ? "excluded" : "included");
      List<BlockWorkerInfo> preferredWorkers =
          mWorkerLocationPolicy.getPreferredWorkers(workers,
              path, mPreferredWorkerCount);
      checkState(!preferredWorkers.isEmpty(),
          "Worker location policy returned no usable worker. Workers available are %s", workers);
      BlockWorkerInfo worker = choosePreferredWorker(preferredWorkers);
      if (!worker.isActive()) {
        throw new RuntimeException("The preferred worker is not active.");
      }
      WorkerNetAddress workerNetAddress = worker.getNetAddress();
      return workerNetAddress;
    } catch (IOException e) {
      // If failed to find workers in the cluster or failed to find the specified number of
      // workers, throw an exception to the application
      throw new RuntimeException(e);
    }
  }

  /**
   * Caches data from ufs.
   * @param ufsPath the full ufs path
   * @param pos the position
   * @param length the length
   */
  public void cacheData(String ufsPath, long pos, long length) {
    // TODO(yimin.wei) break down the load request into loading virtual block requests
    // and load them on different workers.
    try (CloseableResource<BlockWorkerClient> client =
             mContext.acquireBlockWorkerClient(getWorkerNetAddress(ufsPath))) {
      CacheDataRequest request = CacheDataRequest.newBuilder()
          .setUfsPath(ufsPath)
          .setPos(pos)
          .setLength(length)
          .setAsync(true)
          .build();
      client.get().cacheData(request);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Chooses a client preferred worker from multiple workers which hold multiple replicas.
   *
   * @param workers a list of workers
   * @return the preferred worker
   */
  protected BlockWorkerInfo choosePreferredWorker(List<BlockWorkerInfo> workers) {
    return workers.get(0);
  }

  /**
   * Get Context.
   *
   * @return a file system context
   */
  public FileSystemContext getContext() {
    return mContext;
  }
}
