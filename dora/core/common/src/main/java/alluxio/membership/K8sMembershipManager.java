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
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MembershipManager backed by K8s native etcd cluster.
 */
public class K8sMembershipManager implements MembershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(K8sMembershipManager.class);
  private String mClusterName;
  private final KubernetesClient k8sClient = new KubernetesClientBuilder().build();

  /**
   * Create a K8sMembershipManager instance.
   * @param conf
   * @return
   */
  public static K8sMembershipManager create(AlluxioConfiguration conf) {
    return new K8sMembershipManager(conf);
  }
  /**
   * Constructor for K8sMembershipManager.
   */
  public K8sMembershipManager(AlluxioConfiguration conf) {
    mClusterName = conf.getString(PropertyKey.ALLUXIO_CLUSTER_NAME);
  }

  @Override
  public void join(WorkerInfo worker) throws IOException {
    for (Pod workerPod: getAllWorkerPodsInCluster()) {
      if (workerPod.getStatus().getPodIP().equals(worker.getAddress().getHost())) {
        k8sClient.pods().inNamespace("default").withName(workerPod.getMetadata().getName())
            .edit(p -> new PodBuilder(p)
            .editMetadata()
            .addToAnnotations("worker-id", String.valueOf(worker.getId()))
            .endMetadata().build());
      }
      break;
    }
  }

  @Override
  public List<WorkerInfo> getAllMembers() throws IOException {
    List<WorkerInfo> workerInfoList = new ArrayList<>();
    for (Pod workerPod: getAllWorkerPodsInCluster()) {
      workerInfoList.add(new WorkerInfo().setAddress(createWorkerNetAddressFromPod(workerPod)));
    }
    return workerInfoList;
  }

  @Override
  public List<WorkerInfo> getLiveMembers() throws IOException {
    List<WorkerInfo> liveWorkerInfoList = new ArrayList<>();
    for (Pod workerPod: getAllWorkerPodsInCluster()) {
      if (workerPod.getStatus().getContainerStatuses().get(0).getReady()){
        liveWorkerInfoList.add(new WorkerInfo().setAddress(createWorkerNetAddressFromPod(workerPod)));
      }
    }
    return liveWorkerInfoList;
  }

  @Override
  public List<WorkerInfo> getFailedMembers() throws IOException {
    List<WorkerInfo> failedWorkerInfoList = new ArrayList<>();
    for (Pod workerPod: getAllWorkerPodsInCluster()) {
      if (!workerPod.getStatus().getContainerStatuses().get(0).getReady()){
        failedWorkerInfoList.add(new WorkerInfo().setAddress(createWorkerNetAddressFromPod(workerPod)));
      }
    }
    return failedWorkerInfoList;
  }

  @Override
  public String showAllMembers() {
    return "Please run `kubectl get pods -o wide` on K8s control plane "
        + "to see complete worker pod status.";
  }

  @Override
  public void stopHeartBeat(WorkerInfo worker) throws IOException {
    // NOOP
  }

  @Override
  public void decommission(WorkerInfo worker) throws IOException {
    // TO BE IMPLEMENTED
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }

  private List<Pod> getAllWorkerPodsInCluster(){
    return k8sClient.pods()
        .inNamespace(k8sClient.getNamespace())
        .withLabel("release", mClusterName)
        .withLabel("role", "alluxio-worker")
        .list()
        .getItems();
  }

  private WorkerNetAddress createWorkerNetAddressFromPod(Pod workerPod) {
    return new WorkerNetAddress()
      .setHost(workerPod.getStatus().getHostIP())
      .setDataPort(workerPod.getSpec().getContainers().get(0).getPorts().get(0).getContainerPort())
      .setNettyDataPort(workerPod.getSpec().getContainers().get(0).getPorts().get(0).getContainerPort())
      .setRpcPort(workerPod.getSpec().getContainers().get(0).getPorts().get(1).getContainerPort())
      .setWebPort(workerPod.getSpec().getContainers().get(0).getPorts().get(2).getContainerPort());
  }
}
