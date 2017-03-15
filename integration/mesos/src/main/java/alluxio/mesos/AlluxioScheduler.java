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

import com.google.common.base.Joiner;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Class for responding to Mesos offers to launch Alluxio on Mesos.
 */
public class AlluxioScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioScheduler.class);

  private final String mRequiredMasterHostname;

  private boolean mMasterLaunched = false;
  private String mMasterHostname = "";
  private String mTaskName = "";
  private int mMasterTaskId;
  private Set<String> mWorkers = new HashSet<String>();
  int mLaunchedTasks = 0;
  int mMasterCount = 0;

  /**
   * Creates a new {@link AlluxioScheduler}.
   *
   * @param requiredMasterHostname hostname to launch the Alluxio master on; if this is null any
   *        host may be used
   */
  public AlluxioScheduler(String requiredMasterHostname) {
    mRequiredMasterHostname = requiredMasterHostname;
  }

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
  public void registered(final SchedulerDriver driver, Protos.FrameworkID frameworkId,
      Protos.MasterInfo masterInfo) {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        driver.stop();
      }
    }));
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
    long workerOverheadMem =
        Configuration.getBytes(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;
    long ramdiskMem = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE) / Constants.MB;

    LOG.info("Master launched {}, master count {}, "
        + "requested master cpu {} mem {} MB and required master hostname {}",
        mMasterLaunched, mMasterCount, masterCpu, masterMem, mRequiredMasterHostname);

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

      LOG.info("Received offer {} on host {} with cpus {} and mem {} MB and hasMasterPorts {}",
          offer.getId().getValue(), offer.getHostname(), offerCpu, offerMem,
          OfferUtils.hasAvailableMasterPorts(offer));

      Protos.ExecutorInfo.Builder executorBuilder = Protos.ExecutorInfo.newBuilder();
      List<Protos.Resource> resources;
      if (!mMasterLaunched
          && offerCpu >= masterCpu
          && offerMem >= masterMem
          && mMasterCount < Configuration
              .getInt(PropertyKey.INTEGRATION_MESOS_ALLUXIO_MASTER_NODE_COUNT)
          && OfferUtils.hasAvailableMasterPorts(offer)
          && (mRequiredMasterHostname == null
              || mRequiredMasterHostname.equals(offer.getHostname()))) {
        LOG.debug("Creating Alluxio Master executor");
        executorBuilder
            .setName("Alluxio Master Executor")
            .setSource("master")
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue("master"))
            .addAllResources(getExecutorResources())
            .setCommand(
                Protos.CommandInfo
                    .newBuilder()
                    .setValue(createStartAlluxioCommand("alluxio-master-mesos.sh"))
                    .addAllUris(getExecutorDependencyURIList())
                    .setEnvironment(
                        Protos.Environment
                            .newBuilder()
                            .addVariables(
                                Protos.Environment.Variable.newBuilder()
                                    .setName("ALLUXIO_UNDERFS_ADDRESS")
                                    .setValue(Configuration.get(PropertyKey.UNDERFS_ADDRESS))
                                    .build())
                            .addVariables(
                                Protos.Environment.Variable.newBuilder()
                                    .setName("ALLUXIO_MESOS_SITE_PROPERTIES_CONTENT")
                                    .setValue(createAlluxioSiteProperties())
                                    .build())
                            .build()));
        // pre-build resource list here, then use it to build Protos.Task later.
        resources = getMasterRequiredResources(masterCpu, masterMem);
        mMasterHostname = offer.getHostname();
        mTaskName = Configuration.get(PropertyKey.INTEGRATION_MESOS_ALLUXIO_MASTER_NAME);
        mMasterCount++;
        mMasterTaskId = mLaunchedTasks;
      } else if (mMasterLaunched
          && !mWorkers.contains(offer.getHostname())
          && offerCpu >= workerCpu
          && offerMem >= (ramdiskMem + workerOverheadMem)
          && OfferUtils.hasAvailableWorkerPorts(offer)) {
        LOG.debug("Creating Alluxio Worker executor");
        final String memSize = FormatUtils.getSizeFromBytes((long) ramdiskMem * Constants.MB);
        executorBuilder
            .setName("Alluxio Worker Executor")
            .setSource("worker")
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue("worker"))
            .addAllResources(getExecutorResources())
            .setCommand(
                Protos.CommandInfo
                    .newBuilder()
                    .setValue(createStartAlluxioCommand("alluxio-worker-mesos.sh"))
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
                            .addVariables(
                                Protos.Environment.Variable.newBuilder()
                                    .setName("ALLUXIO_MESOS_SITE_PROPERTIES_CONTENT")
                                    .setValue(createAlluxioSiteProperties())
                                    .build())
                            .build()));
        // pre-build resource list here, then use it to build Protos.Task later.
        resources = getWorkerRequiredResources(workerCpu, ramdiskMem + workerOverheadMem);
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

  /**
   * @return the content that should be pasted into an alluxio-site.properties file to recreate the
   *         current configuration
   */
  private String createAlluxioSiteProperties() {
    StringBuilder siteProperties = new StringBuilder();
    for (Entry<String, String> entry : Configuration.toMap().entrySet()) {
      siteProperties.append(String.format("%s=%s%n", entry.getKey(), entry.getValue()));
    }
    return siteProperties.toString();
  }

  private static String createStartAlluxioCommand(String command) {
    List<String> commands = new ArrayList<>();
    commands.add(String.format("echo 'Starting Alluxio with %s'", command));
    if (installJavaFromUrl()) {
      commands
          .add("export JAVA_HOME=" + Configuration.get(PropertyKey.INTEGRATION_MESOS_JDK_PATH));
      commands.add("export PATH=$PATH:$JAVA_HOME/bin");
    }

    commands.add("mkdir conf");
    commands.add("touch conf/alluxio-env.sh");

    // If a jar is supplied, start Alluxio from the jar. Otherwise assume that Alluxio is already
    // installed at PropertyKey.HOME.
    if (installAlluxioFromUrl()) {
      commands.add("rm *.tar.gz");
      // Handle the case where the root directory is named "alluxio" as well as the case where the
      // root directory is named alluxio-$VERSION.
      commands.add("mv alluxio* alluxio_tmp");
      commands.add("mv alluxio_tmp alluxio");
    }
    String home = installAlluxioFromUrl() ? "alluxio" : Configuration.get(PropertyKey.HOME);
    commands
        .add(String.format("cp %s conf", PathUtils.concatPath(home, "conf", "log4j.properties")));
    commands.add(PathUtils.concatPath(home, "integration", "mesos", "bin", command));
    return Joiner.on(" && ").join(commands);
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

  private static List<CommandInfo.URI> getExecutorDependencyURIList() {
    List<URI> dependencies = new ArrayList<>();
    if (installJavaFromUrl()) {
      dependencies.add(CommandInfo.URI.newBuilder()
          .setValue(Configuration.get(PropertyKey.INTEGRATION_MESOS_JDK_URL)).setExtract(true)
          .build());
    }
    if (installAlluxioFromUrl()) {
      dependencies.add(CommandInfo.URI.newBuilder()
          .setValue(Configuration.get(PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL))
          .setExtract(true).build());
    }
    return dependencies;
  }

  private static boolean installJavaFromUrl() {
    return Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_JDK_URL) && !Configuration
        .get(PropertyKey.INTEGRATION_MESOS_JDK_URL).equalsIgnoreCase(Constants.MESOS_LOCAL_INSTALL);
  }

  private static boolean installAlluxioFromUrl() {
    return Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL)
        && !Configuration.get(PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL)
            .equalsIgnoreCase(Constants.MESOS_LOCAL_INSTALL);
  }
}
