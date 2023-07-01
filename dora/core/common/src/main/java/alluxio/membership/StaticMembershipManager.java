package alluxio.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class StaticMembershipManager implements MembershipManager {
  List<WorkerInfo> mMembers;

  private final AlluxioConfiguration mConf;
  public StaticMembershipManager(AlluxioConfiguration conf) throws IOException {
    mConf = conf;
    String workerListFile = conf.getString(PropertyKey.WORKER_MEMBER_STATIC_CONFIG_FILE);
    // user conf/workers, use default port
    mMembers = parseWorkerAddresses(workerListFile, mConf);
  }

  /**
   *
   * @param configFile
   * @param conf
   * @return
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
      String addr = scanner.nextLine();
      addr.trim();
      WorkerNetAddress workerNetAddress = new WorkerNetAddress()
          .setHost(addr)
          .setContainerHost(Configuration.global()
              .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
          .setRpcPort(conf.getInt(PropertyKey.WORKER_RPC_PORT))
          .setWebPort(conf.getInt(PropertyKey.WORKER_WEB_PORT));
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
    String printFormat = "%s\t%s\t%s\n";
    StringBuilder sb = new StringBuilder(
        String.format(printFormat, "WorkerId", "Address", "Status"));
    try {
      for (WorkerInfo worker : getAllMembers()) {
        String entryLine = String.format(printFormat,
            CommonUtils.hashAsStr(worker.getAddress().dumpMainInfo()),
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
