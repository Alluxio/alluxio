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

import alluxio.Constants;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.powermock.reflect.Whitebox;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test utils for {@link FileSystemWorkerClient}.
 */
public class FileSystemWorkerClientTestUtils {
  /**
   * Resets the {@link FileSystemWorkerClient#CLIENT_POOLS} and
   * {@link FileSystemWorkerClient#HEARTBEAT_CLIENT_POOLS}.
   * Resets the {@link FileSystemWorkerClient#HEARTBEAT_CANCEL_POOL} by waiting for all the
   * pending heartbeats.
   */
  public static void reset() {
    CommonUtils.waitFor("All active file system worker sessions are closed",
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            AtomicInteger numActiveSessions = Whitebox
                .getInternalState(FileSystemWorkerClient.class, "NUM_ACTIVE_SESSIONS");
            return numActiveSessions.intValue() == 0;
          }
        }, Constants.MINUTE_MS);

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
