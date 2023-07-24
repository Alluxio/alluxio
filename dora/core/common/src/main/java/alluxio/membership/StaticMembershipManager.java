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

package alluxio.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.HashUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * MembershipManager configured by a static file.
 */
public class StaticMembershipManager implements MembershipManager {
  List<WorkerInfo> mMembers;

  private final AlluxioConfiguration mConf;

  /**
   * CTOR for StaticMembershipManager.
   * @param conf
   * @throws IOException
   */
  public StaticMembershipManager(AlluxioConfiguration conf) throws IOException {
    mConf = conf;
    String workerListFile = conf.getString(PropertyKey.WORKER_MEMBER_STATIC_CONFIG_FILE);
    // user conf/workers, use default port
    mMembers = parseWorkerAddresses(workerListFile, mConf);
  }

  /**
   * Parse the worker addresses from given static config file.
   * The static file only gives the hostname, the rest config params
   * are inherited from given Configuration or default values.
   * @param configFile
   * @param conf
   * @return list of parsed WorkerInfos
   * @throws IOException
   */
  public static List<WorkerInfo> parseWorkerAddresses(
      String configFile, AlluxioConfiguration conf) throws IOException {
    List<WorkerNetAddress> workerAddrs = new ArrayList<>();
    File file = new File(configFile);
    if (!file.exists()) {
      throw new FileNotFoundException("Not found for static worker config file:" + configFile);
    }
    Scanner scanner = new Scanner(file);
    while (scanner.hasNextLine()) {
      String addr = scanner.nextLine().trim();
      WorkerNetAddress workerNetAddress = new WorkerNetAddress()
          .setHost(addr)
          .setContainerHost(Configuration.global()
              .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
          .setRpcPort(conf.getInt(PropertyKey.WORKER_RPC_PORT))
          .setWebPort(conf.getInt(PropertyKey.WORKER_WEB_PORT));
      //data port, these are initialized from configuration for client to deduce the
      //workeraddr related info, on worker side, it will be corrected by join().
      InetSocketAddress inetAddr;
      if (Configuration.global().getBoolean(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED))  {
        inetAddr = NetworkAddressUtils.getBindAddress(
            NetworkAddressUtils.ServiceType.WORKER_DATA,
            Configuration.global());
        workerNetAddress.setNettyDataPort(inetAddr.getPort());
      } else {
        inetAddr = NetworkAddressUtils.getConnectAddress(
            NetworkAddressUtils.ServiceType.WORKER_RPC,
            Configuration.global());
      }
      workerNetAddress.setDataPort(inetAddr.getPort());
      workerAddrs.add(workerNetAddress);
    }
    return workerAddrs.stream()
        .map(w -> new WorkerInfo().setAddress(w)).collect(Collectors.toList());
  }

  @Override
  public void join(WorkerInfo worker) throws IOException {
    // correct with the actual worker addr,
    // same settings such as ports will be applied to other members
    WorkerNetAddress addr = worker.getAddress();
    mMembers.stream().forEach(m -> m.getAddress()
        .setRpcPort(addr.getRpcPort())
        .setDataPort(addr.getDataPort())
        .setDomainSocketPath(addr.getDomainSocketPath())
        .setTieredIdentity(addr.getTieredIdentity())
        .setNettyDataPort(addr.getNettyDataPort())
        .setWebPort(addr.getWebPort())
        .setSecureRpcPort(addr.getSecureRpcPort()));
  }

  @Override
  public List<WorkerInfo> getAllMembers() throws IOException {
    return mMembers;
  }

  @Override
  public List<WorkerInfo> getLiveMembers() throws IOException {
    // No op for static type membership manager
    return mMembers;
  }

  @Override
  public List<WorkerInfo> getFailedMembers() throws IOException {
    // No op for static type membership manager
    return Collections.emptyList();
  }

  @Override
  public String showAllMembers() {
    String printFormat = "%s\t%s\t%s%n";
    StringBuilder sb = new StringBuilder(
        String.format(printFormat, "WorkerId", "Address", "Status"));
    try {
      for (WorkerInfo worker : getAllMembers()) {
        String entryLine = String.format(printFormat,
            HashUtils.hashAsStr(worker.getAddress().dumpMainInfo()),
            worker.getAddress().getHost() + ":" + worker.getAddress().getRpcPort(),
            "N/A");
        sb.append(entryLine);
      }
    } catch (IOException ex) {
      // IGNORE
    }
    return sb.toString();
  }

  @Override
  public void stopHeartBeat(WorkerInfo worker) throws IOException {
    // NOTHING TO DO
  }

  @Override
  public void decommission(WorkerInfo worker) throws IOException {
    mMembers.remove(worker);
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }
}
