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

package alluxio.zookeeper;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.TestingZooKeeperServer;
import org.powermock.reflect.Whitebox;

import java.io.File;

/**
 * Wrapper around to Curator's {@link TestingServer} which allows the server to be restarted.
 */
public final class RestartableTestingServer extends TestingServer {
  private final TestingZooKeeperServer mTestingZooKeeperServer;

  /**
   * Creates the server using the given port.
   *
   * @param port the port
   * @param tempDirectory directory to use
   */
  public RestartableTestingServer(int port, File tempDirectory) throws Exception {
    super(new InstanceSpec(tempDirectory, port, -1, -1, true, -1));
    mTestingZooKeeperServer = Whitebox.getInternalState(this, "testingZooKeeperServer");
  }

  /**
   * Starts the internal testing server.
   */
  public void start() throws Exception {
    mTestingZooKeeperServer.start();
  }
}
