package alluxio.worker.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.Worker;
import alluxio.worker.dora.PagedDoraWorker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StaticMembershipManager implements MembershipManager {
  List<WorkerNetAddress> mMembers;

  private final AlluxioConfiguration mConf;
  public StaticMembershipManager(AlluxioConfiguration conf) {
    mConf = conf;
    List<String> configuredMembers = conf.getList(PropertyKey.WORKER_MEMBER_STATIC_LIST);
    mMembers = parseWorkerAddresses(configuredMembers);
  }

  public static List<WorkerNetAddress> parseWorkerAddresses(List<String> addresses) {
    List<WorkerNetAddress> workerAddrs = new ArrayList<>(addresses.size());
    for (String address : addresses) {
      try {
        InetSocketAddress workerAddr = NetworkAddressUtils.parseInetSocketAddress(address);
        WorkerNetAddress workerNetAddress = new WorkerNetAddress()
            .setHost(workerAddr.getHostName())
            .setRpcPort(workerAddr.getPort());
        workerAddrs.add(workerNetAddress);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to parse host:port: " + address, e);
      }
    }
    return workerAddrs;
  }

  @Override
  public void joinMembership(WorkerNetAddress worker) throws IOException {

  }

  @Override
  public List<WorkerNetAddress> getAllMembers() {
    return mMembers;
  }

  @Override
  public List<WorkerNetAddress> getLiveMembers() {
    // No op for static type membership manager
    return mMembers;
  }

  @Override
  public List<WorkerNetAddress> getFailedMembers() {
    // No op for static type membership manager
    return Collections.emptyList();
  }

  @Override
  public String showAllMembers() {
    String printFormat = "%s\t%s\t%s\n";
    StringBuilder sb = new StringBuilder(
        String.format(printFormat, "WorkerId", "Address", "Status"));
    for (WorkerNetAddress addr : getAllMembers()) {
      String entryLine = String.format(printFormat,
          CommonUtils.hashAsStr(addr.dumpMainInfo()),
          addr.getHost() + ":" + addr.getRpcPort(),
          "N/A");
      sb.append(entryLine);
    }
    return sb.toString();
  }

  @Override
  public void decommission(WorkerNetAddress worker) {
    mMembers.remove(worker);
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }
}
