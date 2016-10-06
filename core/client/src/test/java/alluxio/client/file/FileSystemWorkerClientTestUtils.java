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

package alluxio.client.file;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.powermock.reflect.Whitebox;

import java.net.InetSocketAddress;

/**
 * Test utils for {@link FileSystemWorkerClient}.
 */
public class FileSystemWorkerClientTestUtils {
  /**
   * Resets the {@link FileSystemWorkerClient#CLIENT_POOLS} and
   * {@link FileSystemWorkerClient#HEARTBEAT_CLIENT_POOLS}.
   */
  public static void resetPool() {
    ConcurrentHashMapV8<InetSocketAddress, FileSystemWorkerThriftClientPool> poolMap =
        Whitebox.getInternalState(FileSystemWorkerClient.class, "CLIENT_POOLS");
    for (FileSystemWorkerThriftClientPool pool : poolMap.values()) {
      pool.close();
    }
    poolMap.clear();

    ConcurrentHashMapV8<InetSocketAddress, FileSystemWorkerThriftClientPool> heartbeatPoolMap =
        Whitebox.getInternalState(FileSystemWorkerClient.class, "HEARTBEAT_CLIENT_POOLS");
    for (FileSystemWorkerThriftClientPool pool : heartbeatPoolMap.values()) {
      pool.close();
    }
    heartbeatPoolMap.clear();
  }
}
