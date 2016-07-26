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
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A client to use for interacting with a Block Master.
 */
@ThreadSafe
public interface BlockMasterClient extends Client {

  /**
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  List<WorkerInfo> getWorkerInfoList() throws IOException, ConnectionFailedException;

  /**
   * Returns the {@link BlockInfo} for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the {@link BlockInfo}
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  BlockInfo getBlockInfo(final long blockId) throws AlluxioException, IOException;

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  long getCapacityBytes() throws ConnectionFailedException, IOException;

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  long getUsedBytes() throws ConnectionFailedException, IOException;
}
