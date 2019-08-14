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

import alluxio.Client;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.master.MasterClientContext;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a block master.
 */
@ThreadSafe
public interface BlockMasterClient extends Client {

  /**
   * Factory for {@link BlockMasterClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link BlockMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link BlockMasterClient} instance
     */
    public static BlockMasterClient create(MasterClientContext conf) {
      return new RetryHandlingBlockMasterClient(conf);
    }
  }

  /**
   * Gets the worker information of live workers(support older version Alluxio server).
   *
   * @return a list of worker information
   */
  List<WorkerInfo> getWorkerInfoList() throws IOException;

  /**
   * Gets the worker information of selected workers and selected fields for report CLI.
   *
   * @param options the client defined worker and field ranges
   * @return a list of worker information
   */
  List<WorkerInfo> getWorkerReport(final GetWorkerReportOptions options)
      throws IOException;

  /**
   * @return a list of worker lost storage information
   */
  List<WorkerLostStorageInfo> getWorkerLostStorage() throws IOException;

  /**
   * Returns the {@link BlockInfo} for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the {@link BlockInfo}
   */
  BlockInfo getBlockInfo(final long blockId) throws IOException;

  /**
   * @param fields optional list of fields to query; if null all fields will be queried
   * @return the {@link BlockMasterInfo} block master information
   */
  BlockMasterInfo getBlockMasterInfo(final Set<BlockMasterInfoField> fields) throws IOException;

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   */
  long getCapacityBytes() throws IOException;

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   */
  long getUsedBytes() throws IOException;
}
