package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ConsistentHashPolicyTest {
  InstancedConfiguration mConf;

  @Before
  public void setup() {
    mConf = new InstancedConfiguration(Configuration.copyProperties());
    mConf.set(PropertyKey.USER_WORKER_SELECTION_POLICY,
        "alluxio.client.file.dora.ConsistentHashPolicy");
  }

  @Test
  public void getOneWorker() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof ConsistentHashPolicy);
    // Prepare a worker list
    List<BlockWorkerInfo> workers = new ArrayList<>();
    WorkerNetAddress workerAddr1 = new WorkerNetAddress()
        .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(workerAddr1, 1024, 0));
    WorkerNetAddress workerAddr2 = new WorkerNetAddress()
        .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(workerAddr2, 1024, 0));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 1);
    assertEquals(1, assignedWorkers.size());
    assertTrue(contains(workers, assignedWorkers.get(0)));

    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 1 out of no workers will result in an error
      policy.getPreferredWorkers(ImmutableList.of(), "hdfs://a/b/c", 1);
    });
  }

  @Test
  public void getMultipleWorkers() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof ConsistentHashPolicy);
    // Prepare a worker list
    List<BlockWorkerInfo> workers = new ArrayList<>();
    WorkerNetAddress workerAddr1 = new WorkerNetAddress()
            .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(workerAddr1, 1024, 0));
    WorkerNetAddress workerAddr2 = new WorkerNetAddress()
            .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(workerAddr2, 1024, 0));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 2);
    assertEquals(2, assignedWorkers.size());
    assertTrue(assignedWorkers.stream().allMatch(w -> contains(workers, w)));

    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 2 out of 1 worker will result in an error
      policy.getPreferredWorkers(ImmutableList.of(new BlockWorkerInfo(workerAddr1, 1024, 0)),
          "hdfs://a/b/c", 2);
    });
  }

  private boolean contains(List<BlockWorkerInfo> workers, BlockWorkerInfo targetWorker) {
    // BlockWorkerInfo's equality is delegated to the WorkerNetAddress
    return workers.stream().anyMatch(w ->
        w.getNetAddress().equals(targetWorker.getNetAddress()));
  }
}
