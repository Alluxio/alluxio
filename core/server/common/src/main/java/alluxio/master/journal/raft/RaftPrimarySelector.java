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

import alluxio.master.AbstractPrimarySelector;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.copycat.server.CopycatServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A primary selector backed by a Raft consensus cluster.
 */
@ThreadSafe
public class RaftPrimarySelector extends AbstractPrimarySelector {
  private static final Logger LOG = LoggerFactory.getLogger(RaftPrimarySelector.class);

  private CopycatServer mServer;
  private Listener<CopycatServer.State> mStateListener;

  /**
   * @param server reference to the server backing this selector
   */
  public void init(CopycatServer server) {
    mServer = Preconditions.checkNotNull(server, "server");
    if (mStateListener != null) {
      mStateListener.close();
    }
    // We must register the callback before initializing mState in case the state changes
    // immediately after initializing mState.
    mStateListener = server.onStateChange(state -> {
      setState(serverState());
    });
    setState(serverState());
  }

  private State serverState() {
    if (mServer.state() == CopycatServer.State.LEADER) {
      return State.PRIMARY;
    } else {
      return State.SECONDARY;
    }
  }

  @Override
  public void start(InetSocketAddress address) throws IOException {
    // The copycat cluster is owned by the outer {@link RaftJournalSystem}.
  }

  @Override
  public void stop() throws IOException {
    // The copycat cluster is owned by the outer {@link RaftJournalSystem}.
  }
}
