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

import java.net.InetSocketAddress;

/**
 * A primary selector which allows the user to set the states manually.
 *
 * After creation, you must set the initial state to your desired initial state with
 * {@link AbstractPrimarySelector#setState(alluxio.grpc.NodeState)}.
 */
public class ControllablePrimarySelector extends AbstractPrimarySelector {
  @Override
  public void start(InetSocketAddress localAddress) {
      // nothing to do
  }

  @Override
  public void stop() {
    // nothing to do
  }
}
