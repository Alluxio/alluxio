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

package alluxio.network;

import static org.junit.Assert.fail;

import alluxio.conf.PropertyKey;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for working with ports.
 */
public class PortUtils {

  public static final List<PropertyKey> PROCESS_PORT_LIST =
      Arrays.asList(PropertyKey.MASTER_RPC_PORT, PropertyKey.JOB_MASTER_RPC_PORT,
          PropertyKey.JOB_WORKER_RPC_PORT, PropertyKey.WORKER_RPC_PORT, PropertyKey.MASTER_WEB_PORT,
          PropertyKey.JOB_MASTER_WEB_PORT, PropertyKey.JOB_WORKER_DATA_PORT,
          PropertyKey.PROXY_WEB_PORT, PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT,
          PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, PropertyKey.WORKER_WEB_PORT);

  /**
   * @return a port that is currently free. Note that this port is not reserved and may be taken at
   *         any time
   */
  public static int getFreePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      s.setReuseAddress(true);
      return s.getLocalPort();
    }
  }

  /**
   * Creates a map of property keys to ports using list of ports in PROCESS_PORT_LIST.
   *
   * TODO(zac): find a more reliable way to assign ports for integration testing
   *
   * @return a map property keys to port numbers
   */
  public static Map<PropertyKey, Integer> createPortMapping() {
    Map<PropertyKey, Integer> portMapping = new HashMap<>();
    PROCESS_PORT_LIST.forEach((PropertyKey pk) -> {
      try {
        portMapping.put(pk, PortUtils.getFreePort());
      } catch (IOException e) {
        fail(e.toString());
      }
    });
    return portMapping;
  }
}
