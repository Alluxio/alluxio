package alluxio.worker.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.membership.EtcdClient;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.dora.PagedDoraWorker;
import io.etcd.jetcd.KeyValue;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class EtcdMembershipManager implements MembershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(EtcdMembershipManager.class);
  List<MemberSubscriber> mSubscribers = new ArrayList<>();
  private EtcdClient mEtcdClient;
  private static String mClusterName = "DefaultClusterName";
  private final AlluxioConfiguration mConf;
  private static String sRingPathFormat = "/DHT/%s/AUTHORIZED/";

  public EtcdMembershipManager(AlluxioConfiguration conf) {
    mConf = conf;
//    mClusterName = conf.getString(PropertyKey.CLUSTER_IDENTIFIER_NAME);
    mEtcdClient = new EtcdClient(mClusterName);
    mEtcdClient.connect();
  }

  @Override
  public void close() throws Exception {

  }

  public interface MemberSubscriber {
    public void onViewChange(); // get notified with add/remove nodes
    public void onChange(); // for future for dissemination protocol-like impl to spread info on any changes of a node.
  }

  public void registerRingAndStartSync(PagedDoraWorker.PagedDoraWorkerServiceEntity ctx) throws IOException {
    // 1) register to the ring
    String pathOnRing = String.format(sRingPathFormat, mClusterName) + ctx.getServiceEntityName();
    byte[] ret = mEtcdClient.getForPath(pathOnRing);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    ctx.serialize(dos);
    byte[] serializedEntity = baos.toByteArray();
    // If there's existing entry, check if it's me.
    if (ret != null) {
      // It's not me, something is wrong.
      if (Arrays.compare(serializedEntity, ret) != 0) {
        throw new AlreadyExistsException("Some other member with same id registered on the ring, bail.");
      }
      // It's me, go ahead to start heartbeating.
    } else {
      // If haven't created myself onto the ring before, create now.
      mEtcdClient.createForPath(pathOnRing, Optional.of(serializedEntity));
    }
    // 2) start heartbeat
    mEtcdClient.mServiceDiscovery.registerAndStartSync(ctx);
  }

  private void retrieveFullAndLiveMembers(
      List<PagedDoraWorker.PagedDoraWorkerServiceEntity> authorizedMembers,
      List<PagedDoraWorker.PagedDoraWorkerServiceEntity> liveMembers) {
    String ringPath = String.format(sRingPathFormat, mClusterName);
    List<KeyValue> childrenKvs = mEtcdClient.getChildren(ringPath);
    for (KeyValue kv : childrenKvs) {
      ByteArrayInputStream bais = new ByteArrayInputStream(kv.getValue().getBytes());
      DataInputStream dis = new DataInputStream(bais);
      PagedDoraWorker.PagedDoraWorkerServiceEntity entity = new PagedDoraWorker.PagedDoraWorkerServiceEntity();
      try {
        entity.deserialize(dis);
        authorizedMembers.add(entity);
      } catch (IOException ex) {
        continue;
      }
    }
    for (Map.Entry<String, ByteBuffer> entry : mEtcdClient.mServiceDiscovery
        .getAllLiveServices().entrySet()) {
      ByteBufferInputStream bbis = new ByteBufferInputStream(entry.getValue());
      DataInputStream dis = new DataInputStream(bbis);
      PagedDoraWorker.PagedDoraWorkerServiceEntity entity = new PagedDoraWorker.PagedDoraWorkerServiceEntity();
      try {
        entity.deserialize(dis);
        liveMembers.add(entity);
      } catch (IOException ex) {
        continue;
      }
    }
  }

  public List<WorkerNetAddress> getLiveMembers() {
    List<PagedDoraWorker.PagedDoraWorkerServiceEntity> registeredWorkers = new ArrayList<>();
    List<PagedDoraWorker.PagedDoraWorkerServiceEntity> liveWorkers = new ArrayList<>();
    retrieveFullAndLiveMembers(registeredWorkers, liveWorkers);
    liveWorkers.retainAll(registeredWorkers);
    return liveWorkers.stream().map(e -> e.getWorkerNetAddress()).collect(Collectors.toList());
  }

  public List<WorkerNetAddress> getFailedMembers() {
    List<PagedDoraWorker.PagedDoraWorkerServiceEntity> registeredWorkers = new ArrayList<>();
    List<PagedDoraWorker.PagedDoraWorkerServiceEntity> liveWorkers = new ArrayList<>();
    retrieveFullAndLiveMembers(registeredWorkers, liveWorkers);
    registeredWorkers.removeAll(liveWorkers);
    return registeredWorkers.stream().map(e -> e.getWorkerNetAddress()).collect(Collectors.toList());
  }

  public String showAllMembers() {
    List<PagedDoraWorker.PagedDoraWorkerServiceEntity> registeredWorkers = new ArrayList<>();
    List<PagedDoraWorker.PagedDoraWorkerServiceEntity> liveWorkers = new ArrayList<>();
    retrieveFullAndLiveMembers(registeredWorkers, liveWorkers);
    String printFormat = "%s\t%s\t%s\n";
    StringBuilder sb = new StringBuilder(
        String.format(printFormat, "WorkerId", "Address", "Status"));
    for (PagedDoraWorker.PagedDoraWorkerServiceEntity entity : registeredWorkers) {
      String entryLine = String.format(printFormat,
          entity.getServiceEntityName(),
          entity.getWorkerNetAddress().getHost() + ":" + entity.getWorkerNetAddress().getRpcPort(),
          liveWorkers.contains(entity) ? "ONLINE" : "OFFLINE");
      sb.append(entryLine);
    }
    return sb.toString();
  }

  public void wipeOutClean() {
  }
}
