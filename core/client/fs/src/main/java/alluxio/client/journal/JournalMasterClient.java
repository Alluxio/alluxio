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

package alluxio.client.journal;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.NetAddress;

import java.io.Closeable;

/**
 * Interface for a journal master client.
 */
public interface JournalMasterClient extends Closeable {
  /**
   * Quorum information for participating servers in journal.
   *
   * @return list of server states in quorum
   */
  GetQuorumInfoPResponse getQuorumInfo() throws AlluxioStatusException;

  /**
   * Removes a server from journal quorum.
   *
   * @param serverAddress server address to remove from quorum
   * @throws AlluxioStatusException
   */
  void removeQuorumServer(NetAddress serverAddress) throws AlluxioStatusException;

  /**
   * Initiates changing the leading master of the quorum.
   *
   * @param newLeaderNetAddress server address of the prospective new leader
   * @throws AlluxioStatusException
   */
  void transferLeadership(NetAddress newLeaderNetAddress) throws AlluxioStatusException;

  /**
   * Resets RaftPeer priorities.
   *
   * @throws AlluxioStatusException
   */
  void resetPriorities() throws AlluxioStatusException;
}
