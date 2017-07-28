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
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

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
     * @param masterAddress the master address
     * @return a new {@link BlockMasterClient} instance
     */
    public static BlockMasterClient create(InetSocketAddress masterAddress) {
      return create(null, masterAddress);
    }

    /**
     * Factory method for {@link BlockMasterClient}.
     *
     * @param subject the parent subject
     * @param masterAddress the master address
     * @return a new {@link BlockMasterClient} instance
     */
    public static BlockMasterClient create(Subject subject, InetSocketAddress masterAddress) {
      return RetryHandlingBlockMasterClient.create(subject, masterAddress);
    }
  }

  /**
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   */
  List<WorkerInfo> getWorkerInfoList() throws IOException;

  /**
   * Returns the {@link BlockInfo} for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the {@link BlockInfo}
   */
  BlockInfo getBlockInfo(final long blockId) throws IOException;

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
