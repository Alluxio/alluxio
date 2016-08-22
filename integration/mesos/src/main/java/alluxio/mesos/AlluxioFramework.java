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

package alluxio.mesos;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@link AlluxioFramework} is an implementation of a Mesos framework that is responsible for
 * starting Alluxio processes. The current implementation starts a single Alluxio master and n
 * Alluxio workers (one per Mesos slave).
 *
 * The current resource allocation policy uses a configurable Alluxio master allocation, while the
 * workers use the maximum available allocation.
 */
@NotThreadSafe
public class AlluxioFramework {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Creates a new {@link AlluxioFramework}.
   */
  public AlluxioFramework() {}

  static class AlluxioScheduler implements Scheduler {
    private static final Logger LOG = AlluxioFramework.LOG;
    private boolean mMasterLaunched = false;
    private String mMasterHostname = "";
    private String mTaskName = "";
    private int mMasterTaskId;
    private Set<String> mWorkers = new HashSet<String>();
    int mLaunchedTasks = 0;
    int mMasterCount = 0;

    /**
     * Creates a new {@link AlluxioScheduler}.
     */
    public AlluxioScheduler() {}

    @Override
    public void disconnected(SchedulerDriver driver) {
      LOG.info("Disconnected from master");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
      LOG.error("Error: {}", message);
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId,
        Protos.SlaveID slaveId, int status) {
      LOG.info("Executor {} was lost", executorId.getValue());
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId,
        Protos.SlaveID slaveId, byte[] data) {
      LOG.info("Executor: {}, slave: {}, data: {}",
          executorId.getValue(), slaveId.getValue(), Arrays.toString(data));
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
      LOG.info("Offered {} rescinded", offerId.getValue());
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId,
        Protos.MasterInfo masterInfo) {
      LOG.info("Registered framework {} with master {}:{}",
          frameworkId.getValue(), masterInfo.getHostname(), masterInfo.getPort());
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
      LOG.info("Registered framework with master {}:{}",
          masterInfo.getHostname(), masterInfo.getPort());
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
      long masterCpu = Configuration.getInt(PropertyKey.INTEGRATION_MASTER_RESOURCE_CPU);
      long masterMem =
          Configuration.getBytes(PropertyKey.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB;
      long workerCpu = Configuration.getInt(PropertyKey.INTEGRATION_WORKER_RESOURCE_CPU);
      long workerMem =
          Configuration.getBytes(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;

      LOG.info("Master launched {}, master count {}, "
          + "requested master cpu {} and mem {} MB",
          mMasterLaunched, mMasterCount, masterCpu, masterMem);

      for (Protos.Offer offer : offers) {
        Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();
        double offerCpu = 0;
        double offerMem = 0;
        for (Protos.Resource resource : offer.getResourcesList()) {
          if (resource.getName().equals(Constants.MESOS_RESOURCE_CPUS)) {
            offerCpu += resource.getScalar().getValue();
          } else if (resource.getName().equals(Constants.MESOS_RESOURCE_MEM)) {
            offerMem += resource.getScalar().getValue();
          } else {
            // Other resources are currently ignored.
          }
        }

        LOG.info("Received offer {} with cpus {} and mem {} MB, hasAvailableMasterPorts {}, "
            + "hasAvailableWorkerPorts {}", offer.getId().getValue(), offerCpu, offerMem,
            OfferUtils.hasAvailableMasterPorts(offer),
            OfferUtils.hasAvailableWorkerPorts(offer));

        Protos.ExecutorInfo.Builder executorBuilder = Protos.ExecutorInfo.newBuilder();
        List<Protos.Resource> resources;
        if (!mMasterLaunched && offerCpu >= masterCpu && offerMem >= masterMem
            && mMasterCount < Configuration
            .getInt(PropertyKey.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT) && OfferUtils
            .hasAvailableMasterPorts(offer)) {
          LOG.debug("Creating Alluxio Master executor");
          executorBuilder
              .setName("Alluxio Master Executor")
              .setSource("master")
              .setExecutorId(Protos.ExecutorID.newBuilder().setValue("master"))
              .addAllResources(getExecutorResources())
              .setCommand(
                  Protos.CommandInfo
                      .newBuilder()
                      .setValue(
                          "export JAVA_HOME="
                              + Configuration.get(PropertyKey.INTEGRATION_MESOS_JRE_PATH)
                              + " && export PATH=$PATH:$JAVA_HOME/bin && "
                              + PathUtils.concatPath("alluxio", "integration", "bin",
                              "alluxio-master-mesos.sh"))
                      .addAllUris(getExecutorDependencyURIList())
                      .setEnvironment(
                          Protos.Environment
                              .newBuilder()
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("ALLUXIO_UNDERFS_ADDRESS")
                                      .setValue(Configuration.get(PropertyKey.UNDERFS_ADDRESS))
                                      .build())
                              .build()));
          // pre-build resource list here, then use it to build Protos.Task later.
          resources = getMasterRequiredResources(masterCpu, masterMem);
          mMasterHostname = offer.getHostname();
          mTaskName = Configuration.get(PropertyKey.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME);
          mMasterCount++;
          mMasterTaskId = mLaunchedTasks;

        } else if (mMasterLaunched && !mWorkers.contains(offer.getHostname())
            && offerCpu >= workerCpu && offerMem >= workerMem
            && OfferUtils.hasAvailableWorkerPorts(offer)) {
          LOG.debug("Creating Alluxio Worker executor");
          final String memSize = FormatUtils.getSizeFromBytes((long) workerMem * Constants.MB);
          executorBuilder
              .setName("Alluxio Worker Executor")
              .setSource("worker")
              .setExecutorId(Protos.ExecutorID.newBuilder().setValue("worker"))
              .addAllResources(getExecutorResources())
              .setCommand(
                  Protos.CommandInfo
                      .newBuilder()
                      .setValue(
                          "export JAVA_HOME="
                              + Configuration.get(PropertyKey.INTEGRATION_MESOS_JRE_PATH)
                              + " && export PATH=$PATH:$JAVA_HOME/bin && "
                              + PathUtils.concatPath("alluxio", "integration", "bin",
                              "alluxio-worker-mesos.sh"))
                      .addAllUris(getExecutorDependencyURIList())
                      .setEnvironment(
                          Protos.Environment
                              .newBuilder()
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("ALLUXIO_MASTER_HOSTNAME").setValue(mMasterHostname)
                                      .build())
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("ALLUXIO_WORKER_MEMORY_SIZE").setValue(memSize)
                                      .build())
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("ALLUXIO_UNDERFS_ADDRESS")
                                      .setValue(Configuration.get(PropertyKey.UNDERFS_ADDRESS))
                                      .build())
                              .build()));
          // pre-build resource list here, then use it to build Protos.Task later.
          resources = getWorkerRequiredResources(workerCpu, workerMem);
          mWorkers.add(offer.getHostname());
          mTaskName = Configuration.get(PropertyKey.INTEGRATION_MESOS_ALLUXIO_WORKER_NAME);
        } else {
          // The resource offer cannot be used to start either master or a worker.
          LOG.info("Declining offer {}", offer.getId().getValue());
          driver.declineOffer(offer.getId());
          continue;
        }

        Protos.TaskID taskId =
            Protos.TaskID.newBuilder().setValue(String.valueOf(mLaunchedTasks)).build();

        LOG.info("Launching task {} using offer {}", taskId.getValue(), offer.getId().getValue());

        Protos.TaskInfo task =
            Protos.TaskInfo
                .newBuilder()
                .setName(mTaskName)
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addAllResources(resources)
                .setExecutor(executorBuilder).build();

        launch.addTaskInfos(Protos.TaskInfo.newBuilder(task));
        mLaunchedTasks++;

        // NOTE: We use the new API `acceptOffers` here to launch tasks.
        // The 'launchTasks' API will be deprecated.
        List<Protos.OfferID> offerIds = new ArrayList<Protos.OfferID>();
        offerIds.add(offer.getId());
        List<Protos.Offer.Operation> operations = new ArrayList<Protos.Offer.Operation>();
        Protos.Offer.Operation operation =
            Protos.Offer.Operation.newBuilder().setType(Protos.Offer.Operation.Type.LAUNCH)
                .setLaunch(launch).build();
        operations.add(operation);
        Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
        driver.acceptOffers(offerIds, operations, filters);
      }
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
      // TODO(jiri): Handle lost Mesos slaves.
      LOG.info("Executor {} was lost", slaveId.getValue());
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
      String taskId = status.getTaskId().getValue();
      Protos.TaskState state = status.getState();
      LOG.info("Task {} is in state {}", taskId, state);
      // TODO(jiri): Handle the case when an Alluxio master and/or worker task fails.
      // In particular, we should enable support for the fault tolerant mode of Alluxio to account
      // for Alluxio master process failures and keep track of the running number of Alluxio
      // masters.

      switch (status.getState()) {
        case TASK_FAILED: // intend to fall through
        case TASK_LOST: // intend to fall through
        case TASK_ERROR:
          if (status.getTaskId().getValue().equals(String.valueOf(mMasterTaskId))) {
            mMasterCount--;
          }
          break;
        case TASK_RUNNING:
          if (status.getTaskId().getValue().equals(String.valueOf(mMasterTaskId))) {
            mMasterLaunched = true;
          }
          break;
        default:
          break;
      }
    }

    private List<Protos.Resource> getMasterRequiredResources(long masterCpus, long masterMem) {
      List<Protos.Resource> resources = getCoreRequiredResouces(masterCpus, masterMem);
      // Set master rcp port, web ui port, data port as range resources for this task.
      // By default, it would require 19998, 19999 ports for master process.
      resources.add(Protos.Resource.newBuilder()
          .setName(Constants.MESOS_RESOURCE_PORTS)
          .setType(Protos.Value.Type.RANGES)
          .setRanges(Protos.Value.Ranges.newBuilder()
              .addRange(Protos.Value.Range.newBuilder()
                  .setBegin(Configuration.getLong(PropertyKey.MASTER_WEB_PORT))
                  .setEnd(Configuration.getLong(PropertyKey.MASTER_WEB_PORT)))
              .addRange((Protos.Value.Range.newBuilder()
                  .setBegin(Configuration.getLong(PropertyKey.MASTER_RPC_PORT))
                  .setEnd(Configuration.getLong(PropertyKey.MASTER_RPC_PORT))))).build());
      return resources;
    }

    private List<Protos.Resource> getWorkerRequiredResources(long workerCpus, long workerMem) {
      List<Protos.Resource> resources = getCoreRequiredResouces(workerCpus, workerMem);
      // Set worker rcp port, web ui port, data port as range resources for this task.
      // By default, it would require 29998, 29999, 30000 ports for worker process.
      resources.add(Protos.Resource.newBuilder()
          .setName(Constants.MESOS_RESOURCE_PORTS)
          .setType(Protos.Value.Type.RANGES)
          .setRanges(Protos.Value.Ranges.newBuilder()
              .addRange(Protos.Value.Range.newBuilder()
                  .setBegin(Configuration.getLong(PropertyKey.WORKER_RPC_PORT))
                  .setEnd(Configuration.getLong(PropertyKey.WORKER_RPC_PORT)))
              .addRange(Protos.Value.Range.newBuilder()
                  .setBegin(Configuration.getLong(PropertyKey.WORKER_DATA_PORT))
                  .setEnd(Configuration.getLong(PropertyKey.WORKER_DATA_PORT)))
              .addRange((Protos.Value.Range.newBuilder()
                  .setBegin(Configuration.getLong(PropertyKey.WORKER_WEB_PORT))
                  .setEnd(Configuration.getLong(PropertyKey.WORKER_WEB_PORT))))).build());
      return resources;
    }

    private List<Protos.Resource> getCoreRequiredResouces(long cpus, long mem) {
      // Build cpu/mem resource for task.
      List<Protos.Resource> resources = new ArrayList<Protos.Resource>();
      resources.add(Protos.Resource.newBuilder()
          .setName(Constants.MESOS_RESOURCE_CPUS)
          .setType(Protos.Value.Type.SCALAR)
          .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus)).build());
      resources.add(Protos.Resource.newBuilder()
          .setName(Constants.MESOS_RESOURCE_MEM)
          .setType(Protos.Value.Type.SCALAR)
          .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem)).build());
      return resources;
    }

    private List<Protos.Resource> getExecutorResources() {
      // JIRA: https://issues.apache.org/jira/browse/MESOS-1807
      // From Mesos 0.22.0, executors must set CPU resources to at least 0.01 and
      // memory resources to at least 32MB.
      List<Protos.Resource> resources = new ArrayList<Protos.Resource>(2);
      // Both cpus/mem are "scalar" type, which means a double value should be used.
      // The resource name is "cpus", type is scalar and the value is 0.1 to tell Mesos
      // this executor would allocate 0.1 cpu for itself.
      resources.add(Protos.Resource.newBuilder().setName(Constants.MESOS_RESOURCE_CPUS)
          .setType(Protos.Value.Type.SCALAR)
          .setScalar(Protos.Value.Scalar.newBuilder().setValue(0.1d)).build());
      // The resource name is "mem", type is scalar and the value is 32.0MB to tell Mesos
      // this executor would allocate 32.0MB mem for itself.
      resources.add(Protos.Resource.newBuilder().setName(Constants.MESOS_RESOURCE_MEM)
          .setType(Protos.Value.Type.SCALAR)
          .setScalar(Protos.Value.Scalar.newBuilder().setValue(32.0d)).build());
      return resources;
    }
  }

  private static void usage() {
    String name = AlluxioFramework.class.getName();
    LOG.error("This is an implementation of a Mesos framework that is responsible for "
        + "starting Alluxio processes. The current implementation starts a single Alluxio master "
        + "and n Alluxio workers (one per Mesos slave).");
    LOG.error("Usage: {} <hostname>", name);
  }

  private static List<CommandInfo.URI> getExecutorDependencyURIList() {
    String dependencyPath =
        Configuration.get(PropertyKey.INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH);
    return Lists.newArrayList(
        CommandInfo.URI.newBuilder()
            .setValue(PathUtils.concatPath(dependencyPath, "alluxio.tar.gz")).setExtract(true)
            .build(), CommandInfo.URI.newBuilder()
            .setValue(Configuration.get(PropertyKey.INTEGRATION_MESOS_JRE_URL)).setExtract(true)
            .build());
  }

  private static Protos.Credential createCredential() {

    if (!Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_PRINCIPAL)) {
      return null;
    }

    try {
      Protos.Credential.Builder credentialBuilder = Protos.Credential.newBuilder()
          .setPrincipal(Configuration.get(PropertyKey.INTEGRATION_MESOS_PRINCIPAL)).setSecret(
              ByteString.copyFrom(
                  Configuration.get(PropertyKey.INTEGRATION_MESOS_SECRET).getBytes("UTF-8")));

      return credentialBuilder.build();
    } catch (UnsupportedEncodingException ex) {
      LOG.error("Failed to encode secret when creating Credential.", ex);
    }
    return null;
  }

  /**
   * Starts the Alluxio framework.
   *
   * @param args command-line arguments
   * @throws Exception if the Alluxio framework encounters an unrecoverable error
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      usage();
      System.exit(1);
    }
    String hostname = args[0];

    // Start Mesos master. Setting the user to an empty string will prompt Mesos to set it to the
    // current user.
    Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
        .setName("alluxio").setCheckpoint(true);

    if (Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_ROLE)) {
      frameworkInfo.setRole(Configuration.get(PropertyKey.INTEGRATION_MESOS_ROLE));
    }

    if (Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_USER)) {
      frameworkInfo.setUser(Configuration.get(PropertyKey.INTEGRATION_MESOS_USER));
    }

    if (Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_PRINCIPAL)) {
      frameworkInfo.setPrincipal(Configuration.get(PropertyKey.INTEGRATION_MESOS_PRINCIPAL));
    }

    Scheduler scheduler = new AlluxioScheduler();

    Protos.Credential cred = createCredential();
    MesosSchedulerDriver driver;
    if (cred == null) {
      driver = new MesosSchedulerDriver(scheduler, frameworkInfo.build(), hostname);
    } else {
      driver = new MesosSchedulerDriver(scheduler, frameworkInfo.build(), hostname, cred);
    }

    int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    System.exit(status);
  }
}
