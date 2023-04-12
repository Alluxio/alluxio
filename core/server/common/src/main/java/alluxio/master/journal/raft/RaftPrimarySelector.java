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

package alluxio.master.journal.raft;

import alluxio.grpc.NodeState;
import alluxio.master.AbstractPrimarySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A primary selector backed by a Raft consensus cluster.
 */
@ThreadSafe
public class RaftPrimarySelector extends AbstractPrimarySelector {
  private static final Logger LOG = LoggerFactory.getLogger(RaftPrimarySelector.class);

  /**
   * Notifies leadership state changed.
   * @param state the leadership state
   */
  public void notifyStateChanged(NodeState state) {
    setState(state);
  }

  @Override
  public void start(InetSocketAddress address) {
    // The Ratis cluster is owned by the outer {@link RaftJournalSystem}.
    LOG.info("Job master Raft journal started in state {}", getState());
  }

  @Override
  public void stop() {
    // The Ratis cluster is owned by the outer {@link RaftJournalSystem}.
  }
}
