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

package alluxio.client.block;

import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.collections.Pair;
import alluxio.network.TieredIdentityFactory;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.Map;

/**
 * Alluxio Block Store client. This is an internal client for all block level operations in Alluxio.
 * An instance of this class can be obtained via {@link BlockStoreClient} constructors.
 */
public interface BlockStoreClient {

  /**
   * Creates an Alluxio block store with default local hostname.
   *
   * @param context the file system context
   * @return the {@link BlockStoreClient} created
   */
  static BlockStoreClient create(FileSystemContext context) {
    return new DefaultBlockStoreClient(context,
            TieredIdentityFactory.localIdentity(context.getClusterConf()));
  }

  /**
   * Gets the block info of a block, if it exists.
   *
   * @param blockId the blockId to obtain information about
   * @return a {@link BlockInfo} containing the metadata of the block
   */
  BlockInfo getInfo(long blockId) throws IOException;

  /**
   * Gets a stream to read the data of a block. This method is primarily responsible for
   * determining the data source and type of data source. The latest BlockInfo will be fetched
   * from the master to ensure the locations are up-to-date.
   *
   * @param blockId the id of the block to read
   * @param options the options associated with the read request
   * @return a stream which reads from the beginning of the block
   */
  BlockInStream getInStream(long blockId, InStreamOptions options) throws IOException;

  /**
   * Gets a stream to read the data of a block. This method is primarily responsible for
   * determining the data source and type of data source. The latest BlockInfo will be fetched
   * from the master to ensure the locations are up-to-date. It takes a map of failed workers and
   * their most recently failed time and tries to update it when BlockInStream created failed,
   * attempting to avoid reading from a recently failed worker.
   *
   * @param blockId the id of the block to read
   * @param options the options associated with the read request
   * @param failedWorkers the map of workers addresses to most recent failure time
   * @return a stream which reads from the beginning of the block
   */
  BlockInStream getInStream(long blockId, InStreamOptions options,
      Map<WorkerNetAddress, Long> failedWorkers) throws IOException;

  /**
   * {@link #getInStream(long, InStreamOptions, Map)}.
   *
   * @param info the block info
   * @param options the options associated with the read request
   * @param failedWorkers the map of workers addresses to most recent failure time
   * @return a stream which reads from the beginning of the block
   */
  BlockInStream getInStream(BlockInfo info, InStreamOptions options,
      Map<WorkerNetAddress, Long> failedWorkers) throws IOException;

  /**
   * Gets the data source and type of data source of a block. This method is primarily responsible
   * for determining the data source and type of data source. It takes a map of failed workers and
   * their most recently failed time and tries to update it when BlockInStream created failed,
   * attempting to avoid reading from a recently failed worker.
   *
   * @param info the info of the block to read
   * @param status the URIStatus associated with the read request
   * @param policy the policy determining the Alluxio worker location
   * @param failedWorkers the map of workers addresses to most recent failure time
   * @return the data source and type of data source of the block
   */
  Pair<WorkerNetAddress, BlockInStreamSource> getDataSourceAndType(BlockInfo info,
      URIStatus status, BlockLocationPolicy policy, Map<WorkerNetAddress, Long> failedWorkers)
      throws IOException;

  /**
   * Gets a stream to write data to a block. The stream can only be backed by Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write
   * @param address the address of the worker to write the block to, fails if the worker cannot
   *        serve the request
   * @param options the output stream options
   * @return an {@link BlockOutStream} which can be used to write data to the block in a streaming
   *         fashion
   */
  BlockOutStream getOutStream(long blockId, long blockSize, WorkerNetAddress address,
      OutStreamOptions options) throws IOException;

  /**
   * Gets a stream to write data to a block based on the options. The stream can only be backed by
   * Alluxio storage.
   *
   * @param blockId the block to write
   * @param blockSize the standard block size to write
   * @param options the output stream option
   * @return a {@link BlockOutStream} which can be used to write data to the block in a streaming
   *         fashion
   */
  BlockOutStream getOutStream(long blockId, long blockSize, OutStreamOptions options)
      throws IOException;

  /**
   * Gets the total capacity of Alluxio's BlockStore.
   *
   * @return the capacity in bytes
   */
  long getCapacityBytes() throws IOException;

  /**
   * Gets the used bytes of Alluxio's BlockStore.
   *
   * @return the used bytes of Alluxio's BlockStore
   */
  long getUsedBytes() throws IOException;
}
