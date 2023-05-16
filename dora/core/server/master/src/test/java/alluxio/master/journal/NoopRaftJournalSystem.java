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

import alluxio.master.Master;
import alluxio.master.journal.noop.NoopJournal;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.util.network.NetworkAddressUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * A noop raft journal system for testing.
 */
public class NoopRaftJournalSystem extends RaftJournalSystem {
  private boolean mIsLeader = false;

  /**
   * Creates a raft journal system object.
   * @throws URISyntaxException
   */
  public NoopRaftJournalSystem() throws URISyntaxException {
    super(new URI(""), NetworkAddressUtils.ServiceType.MASTER_RAFT);
  }

  /**
   * Sets the raft journal state.
   * @param isLeader if the raft journal system should be a leader
   */
  public synchronized void setIsLeader(boolean isLeader) {
    mIsLeader = isLeader;
  }

  @Override
  public synchronized void start() {
  }

  @Override
  public synchronized void stop() {
  }

  @Override
  public synchronized boolean isLeader() {
    return mIsLeader;
  }

  @Override
  public synchronized void startInternal() {
  }

  @Override
  public synchronized void stopInternal() {
  }

  @Override
  public synchronized void gainPrimacy() {
  }

  @Override
  public synchronized void losePrimacy() {
  }

  @Override
  public synchronized Journal createJournal(Master master) {
    return new NoopJournal();
  }
}
