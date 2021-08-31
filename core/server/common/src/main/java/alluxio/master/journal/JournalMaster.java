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

package alluxio.master.journal;

import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.NetAddress;

import java.io.IOException;

/**
 * Interface for journal master implementations.
 */
public interface JournalMaster {
  /**
   * Quorum information for participating servers in journal. This method is supported only for
   * {@link alluxio.master.journal.JournalType#EMBEDDED} journal.
   *
   * @return the quorum info
   * @throws IOException if error occurs while performing the operation
   */
  GetQuorumInfoPResponse getQuorumInfo() throws IOException;

  /**
   * Removes a server from journal quorum. This method is supported only for
   * {@link alluxio.master.journal.JournalType#EMBEDDED} journal.
   *
   * @param serverAddress server address to remove from quorum
   * @throws IOException if error occurs while performing the operation
   */
  void removeQuorumServer(NetAddress serverAddress) throws IOException;

  /**
   * Changes the leading master of the journal quorum. This method is supported only for
   * {@link alluxio.master.journal.JournalType#EMBEDDED} journal.
   *
   * @param newLeaderAddress server address to remove from quorum
   * @throws IOException if error occurs while performing the operation
   */
  void transferLeadership(NetAddress newLeaderAddress) throws IOException;

  /**
   * Resets RaftPeer priorities.
   *
   * @throws IOException if error occurs while performing the operation
   */
  void resetPriorities() throws IOException;
}
