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

import alluxio.AbstractResourceRule;

import com.google.common.base.Preconditions;

/**
 * Rule for reserving a port during a test. Other tests will not attempt to use this port.
 */
public class PortReservationRule extends AbstractResourceRule {
  private int mPort = -1;

  @Override
  public void before() {
    mPort = PortRegistry.reservePort();
  }

  @Override
  public void after() {
    PortRegistry.release(mPort);
    mPort = -1;
  }

  public int getPort() {
    Preconditions.checkState(mPort >= 0,
        "cannot call getPort() before the rule has been activated");
    return mPort;
  }
}
