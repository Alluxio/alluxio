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
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NetAddress;
import alluxio.master.journal.raft.RaftJournalSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default implementation for {@link JournalMaster}. This is used by both master and job_master
 * processes.
 */
public class DefaultJournalMaster implements JournalMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultJournalMaster.class);

  private JournalDomain mJournalDomain;
  private JournalSystem mJournalSystem;

  /**
   * @param journalDomain domain for the journal
   * @param journalSystem internal {@link JournalSystem} reference
   */
  public DefaultJournalMaster(JournalDomain journalDomain, JournalSystem journalSystem) {
    mJournalDomain = journalDomain;
    mJournalSystem = journalSystem;
  }

  private void checkQuorumOpSupported() {
    if (!(mJournalSystem instanceof RaftJournalSystem)) {
      throw new UnsupportedOperationException(
          "Quorum operations are supported for journal type: EMBEDDED");
    }
  }

  @Override
  public GetQuorumInfoPResponse getQuorumInfo() throws IOException {
    checkQuorumOpSupported();
    return GetQuorumInfoPResponse.newBuilder().setDomain(mJournalDomain)
        .addAllServerInfo(((RaftJournalSystem) mJournalSystem).getQuorumServerInfoList()).build();
  }

  @Override
  public void removeQuorumServer(NetAddress serverAddress) throws IOException {
    checkQuorumOpSupported();
    ((RaftJournalSystem) mJournalSystem).removeQuorumServer(serverAddress);
  }

  @Override
  public void transferLeadership(NetAddress newLeaderAddress) throws IOException {
    checkQuorumOpSupported();
    ((RaftJournalSystem) mJournalSystem).transferLeadership(newLeaderAddress);
  }

  @Override
  public void resetPriorities() throws IOException {
    checkQuorumOpSupported();
    ((RaftJournalSystem) mJournalSystem).resetPriorities();
  }
}
