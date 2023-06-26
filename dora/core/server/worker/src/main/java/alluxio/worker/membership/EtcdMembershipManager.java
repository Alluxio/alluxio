package alluxio.worker.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.membership.AlluxioEtcdClient;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.Worker;
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
  private AlluxioEtcdClient mAlluxioEtcdClient;
  private static String mClusterName;
  private final AlluxioConfiguration mConf;
  private static String sRingPathFormat = "/DHT/%s/AUTHORIZED/";

  public EtcdMembershipManager(AlluxioConfiguration conf) {
    mConf = conf;
    mClusterName = conf.getString(PropertyKey.ALLUXIO_CLUSTER_NAME);
    mAlluxioEtcdClient = AlluxioEtcdClient.getInstance(conf);
  }

  public void join(WorkerNetAddress wkrAddr) throws IOException {
    WorkerServiceEntity entity = new WorkerServiceEntity(wkrAddr);
    // 1) register to the ring
    String pathOnRing = String.format(sRingPathFormat, mClusterName) + entity.getServiceEntityName();
    byte[] ret = mAlluxioEtcdClient.getForPath(pathOnRing);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    entity.serialize(dos);
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
      mAlluxioEtcdClient.createForPath(pathOnRing, Optional.of(serializedEntity));
    }
    // 2) start heartbeat
    mAlluxioEtcdClient.mServiceDiscovery.registerAndStartSync(entity);
  }

  public List<WorkerNetAddress> getAllMembers() {
    List<WorkerServiceEntity> registeredWorkers = retrieveFullMembers();
    return registeredWorkers.stream().map(e -> e.getWorkerNetAddress()).collect(Collectors.toList());
  }

  private List<WorkerServiceEntity> retrieveFullMembers() {
    List<WorkerServiceEntity> fullMembers = new ArrayList<>();
    String ringPath = String.format(sRingPathFormat, mClusterName);
    List<KeyValue> childrenKvs = mAlluxioEtcdClient.getChildren(ringPath);
    for (KeyValue kv : childrenKvs) {
      ByteArrayInputStream bais = new ByteArrayInputStream(kv.getValue().getBytes());
      DataInputStream dis = new DataInputStream(bais);
      WorkerServiceEntity entity = new WorkerServiceEntity();
      try {
        entity.deserialize(dis);
        fullMembers.add(entity);
      } catch (IOException ex) {
        // Ignore
      }
    }
    return fullMembers;
  }

  private List<WorkerServiceEntity> retrieveLiveMembers() {
    List<WorkerServiceEntity> liveMembers = new ArrayList<>();
    for (Map.Entry<String, ByteBuffer> entry : mAlluxioEtcdClient.mServiceDiscovery
        .getAllLiveServices().entrySet()) {
      ByteBufferInputStream bbis = new ByteBufferInputStream(entry.getValue());
      DataInputStream dis = new DataInputStream(bbis);
      WorkerServiceEntity entity = new WorkerServiceEntity();
      try {
        entity.deserialize(dis);
        liveMembers.add(entity);
      } catch (IOException ex) {
        // Ignore
      }
    }
    return liveMembers;
  }

  public List<WorkerNetAddress> getLiveMembers() {
    List<WorkerServiceEntity> registeredWorkers = retrieveFullMembers();
    List<WorkerServiceEntity> liveWorkers = retrieveLiveMembers();
    liveWorkers.retainAll(registeredWorkers);
    return liveWorkers.stream().map(e -> e.getWorkerNetAddress()).collect(Collectors.toList());
  }

  public List<WorkerNetAddress> getFailedMembers() {
    List<WorkerServiceEntity> registeredWorkers = retrieveFullMembers();
    List<WorkerServiceEntity> liveWorkers = retrieveLiveMembers();
    registeredWorkers.removeAll(liveWorkers);
    return registeredWorkers.stream().map(e -> e.getWorkerNetAddress()).collect(Collectors.toList());
  }

  public String showAllMembers() {
    List<WorkerServiceEntity> registeredWorkers = retrieveFullMembers();
    List<WorkerServiceEntity> liveWorkers = retrieveLiveMembers();
    String printFormat = "%s\t%s\t%s\n";
    StringBuilder sb = new StringBuilder(
        String.format(printFormat, "WorkerId", "Address", "Status"));
    for (WorkerServiceEntity entity : registeredWorkers) {
      String entryLine = String.format(printFormat,
          entity.getServiceEntityName(),
          entity.getWorkerNetAddress().getHost() + ":" + entity.getWorkerNetAddress().getRpcPort(),
          liveWorkers.contains(entity) ? "ONLINE" : "OFFLINE");
      sb.append(entryLine);
    }
    return sb.toString();
  }

  @Override
  public void decommission(WorkerNetAddress worker) {
    // TO BE IMPLEMENTED
  }

  @Override
  public void close() throws Exception {
    mAlluxioEtcdClient.close();
  }
}
