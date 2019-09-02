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

package alluxio.client.util;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for the client tests.
 */
public final class ClientTestUtils {

  /**
   * Sets small buffer sizes so that Alluxio does not run out of heap space.
   */
  public static void setSmallBufferSizes(InstancedConfiguration conf) {
    conf.set(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "4KB");
    conf.set(PropertyKey.USER_FILE_BUFFER_BYTES, "4KB");
  }

  /**
   * Resets the client to its initial state, re-initializing Alluxio contexts.
   *
   * This method should only be used as a cleanup mechanism between tests.
   */
  public static void resetClient(InstancedConfiguration conf) {
    try {
      resetContexts(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void resetContexts(InstancedConfiguration conf) throws IOException {
    conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
  }

  public static BlockWorkerInfo worker(long capacity, String node, String rack) {
    return worker(capacity, 0, node, rack);
  }

  public static BlockWorkerInfo worker(long capacity, long used, String node, String rack) {
    WorkerNetAddress address = new WorkerNetAddress();
    List<LocalityTier> tiers = new ArrayList<>();
    if (node != null && !node.isEmpty()) {
      address.setHost(node);
      tiers.add(new LocalityTier(Constants.LOCALITY_NODE, node));
    }
    if (rack != null && !rack.isEmpty()) {
      tiers.add(new LocalityTier(Constants.LOCALITY_RACK, rack));
    }
    address.setTieredIdentity(new TieredIdentity(tiers));
    return new BlockWorkerInfo(address, capacity, used);
  }
}
