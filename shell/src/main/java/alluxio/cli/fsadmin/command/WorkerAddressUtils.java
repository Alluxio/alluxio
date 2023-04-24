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

package alluxio.cli.fsadmin.command;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkerAddressUtils {

  public static List<WorkerNetAddress> parseWorkerAddresses(
      String workerAddressesStr, AlluxioConfiguration alluxioConf) {
    List<WorkerNetAddress> result = new ArrayList<>();
    for (String part : workerAddressesStr.split(",")) {
      if (part.contains(":")) {
        String[] p = part.split(":");
        Preconditions.checkState(p.length == 2,
            "worker address %s cannot be recognized", part);
        String port = p[1];
        WorkerNetAddress addr = new WorkerNetAddress()
                .setHost(p[0]).setWebPort(Integer.parseInt(port));
        result.add(addr);
      } else {
        int port = alluxioConf.getInt(PropertyKey.WORKER_WEB_PORT);
        WorkerNetAddress addr = new WorkerNetAddress().setHost(part).setWebPort(port);
        result.add(addr);
      }
    }
    return result;
  }

  public static String workerAddressListToString(Collection<WorkerNetAddress> workers) {
    return workers.stream().map(WorkerAddressUtils::convertAddressToStringWebPort)
            .collect(Collectors.toList()).toString();
  }


  public static String workerListToString(Set<BlockWorkerInfo> worker) {
    if (worker.isEmpty()) {
      return "[]";
    }
    // Print on a new line
    return "\n" + worker.stream().map(w -> convertAddressToStringWebPort(w.getNetAddress()))
        .collect(Collectors.toList());
  }

  // To stay consistent with the command, we print the web port of each worker
  public static String convertAddressToStringWebPort(WorkerNetAddress address) {
    return address.getHost() + ":" + address.getWebPort();
  }
}
