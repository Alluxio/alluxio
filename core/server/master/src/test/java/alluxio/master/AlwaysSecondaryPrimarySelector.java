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

package alluxio.master;

import alluxio.util.interfaces.Scoped;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

/**
 * A primary selector which is always secondary.
 */
public final class AlwaysSecondaryPrimarySelector implements PrimarySelector {
  @Override
  public void start(InetSocketAddress localAddress) throws IOException {
    // Nothing to do.
  }

  @Override
  public void stop() throws IOException {
    // Nothing to do.
  }

  @Override
  public State getState() {
    return State.SECONDARY;
  }

  @Override
  public Scoped onStateChange(Consumer<State> listener) {
    // State never changes.
    return () -> { };
  }

  @Override
  public void waitForState(State state) throws InterruptedException {
    switch (state) {
      case PRIMARY:
        // Never happening
        Thread.sleep(Long.MAX_VALUE);
        break;
      case SECONDARY:
        return;
      default:
        throw new IllegalStateException("Unknown primary selector state: " + state);
    }
  }
}
