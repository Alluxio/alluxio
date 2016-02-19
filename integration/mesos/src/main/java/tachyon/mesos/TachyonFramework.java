/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.mesos;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

/**
 * {@link TachyonFramework} is an implementation of a Mesos framework that is responsible for
 * starting Tachyon processes. The current implementation starts a single Tachyon master and n
 * Tachyon workers (one per Mesos slave).
 *
 * The current resource allocation policy uses a configurable Tachyon master allocation, while the
 * workers use the maximum available allocation.
 */
@NotThreadSafe
public class TachyonFramework {
  static class TachyonScheduler implements Scheduler {
    private static TachyonConf sConf = new TachyonConf();
    private boolean mMasterLaunched = false;
    private String mMasterHostname = "";
    private String mTaskName = "";
    private int mMasterTaskId;
    private Set<String> mWorkers = new HashSet<String>();
    int mLaunchedTasks = 0;
    int mMasterCount = 0;

    @Override
    public void disconnected(SchedulerDriver driver) {
      System.out.println("Disconnected from master.");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
      System.out.println("Error: " + message);
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId,
        Protos.SlaveID slaveId, int status) {
      System.out.println("Executor " + executorId.getValue() + " was lost.");
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId,
        Protos.SlaveID slaveId, byte[] data) {
      System.out.println("Executor: " + executorId.getValue() + ", " + "Slave: "
          + slaveId.getValue() + ", " + "Data: " + Arrays.toString(data) + ".");
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
      System.out.println("Offered " + offerId.getValue() + " rescinded.");
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId,
        Protos.MasterInfo masterInfo) {
      System.out.println("Registered framework " + frameworkId.getValue() + " with master "
          + masterInfo.getHostname() + ":" + masterInfo.getPort() + ".");
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
      System.out.println("Registered framework with master " + masterInfo.getHostname() + ":"
          + masterInfo.getPort() + ".");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
      long masterCpu = sConf.getInt(Constants.INTEGRATION_MASTER_RESOURCE_CPU);
      long masterMem = sConf.getBytes(Constants.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB;
      long workerCpu = sConf.getInt(Constants.INTEGRATION_WORKER_RESOURCE_CPU);
      long workerMem = sConf.getBytes(Constants.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;

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

        System.out.println("Received offer " + offer.getId().getValue() + " with cpus: " + offerCpu
            + " and mem: " + offerMem + "MB.");

        Protos.ExecutorInfo.Builder executorBuilder = Protos.ExecutorInfo.newBuilder();
        double targetCpu;
        double targetMem;
        if (!mMasterLaunched && offerCpu >= masterCpu && offerMem >= masterMem
            && mMasterCount < sConf.getInt(Constants.INTEGRATION_MESOS_TACHYON_MASTER_NODE_COUNT)) {
          executorBuilder
              .setName("Tachyon Master Executor")
              .setSource("master")
              .setExecutorId(Protos.ExecutorID.newBuilder().setValue("master"))
              .setCommand(
                  Protos.CommandInfo
                      .newBuilder()
                      .setValue(
                          "export JAVA_HOME="
                              + sConf.get(Constants.INTEGRATION_MESOS_JRE_PATH)
                              + " && export PATH=$PATH:$JAVA_HOME/bin && "
                              + PathUtils.concatPath("tachyon", "integration", "bin",
                                  "tachyon-master-mesos.sh"))
                      .addAllUris(getExecutorDependencyURIList())
                      .setEnvironment(
                          Protos.Environment
                              .newBuilder()
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("TACHYON_UNDERFS_ADDRESS")
                                      .setValue(sConf.get(Constants.UNDERFS_ADDRESS))
                                      .build())
                              .build()));
          targetCpu = masterCpu;
          targetMem = masterMem;
          mMasterHostname = offer.getHostname();
          mTaskName = sConf.get(Constants.INTEGRATION_MESOS_TACHYON_MASTER_NAME);
          mMasterCount ++;
          mMasterTaskId = mLaunchedTasks;

        } else if (mMasterLaunched && !mWorkers.contains(offer.getHostname())
            && offerCpu >= workerCpu && offerMem >= workerMem) {
          final String memSize = FormatUtils.getSizeFromBytes((long) workerMem * Constants.MB);
          executorBuilder
              .setName("Tachyon Worker Executor")
              .setSource("worker")
              .setExecutorId(Protos.ExecutorID.newBuilder().setValue("worker"))
              .setCommand(
                  Protos.CommandInfo
                      .newBuilder()
                      .setValue(
                          "export JAVA_HOME="
                              + sConf.get(Constants.INTEGRATION_MESOS_JRE_PATH)
                              + " && export PATH=$PATH:$JAVA_HOME/bin && "
                              + PathUtils.concatPath("tachyon", "integration", "bin",
                                  "tachyon-worker-mesos.sh"))
                      .addAllUris(getExecutorDependencyURIList())
                      .setEnvironment(
                          Protos.Environment
                              .newBuilder()
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("TACHYON_MASTER_ADDRESS").setValue(mMasterHostname)
                                      .build())
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("TACHYON_WORKER_MEMORY_SIZE").setValue(memSize)
                                      .build())
                              .addVariables(
                                  Protos.Environment.Variable.newBuilder()
                                      .setName("TACHYON_UNDERFS_ADDRESS")
                                      .setValue(sConf.get(Constants.UNDERFS_ADDRESS))
                                      .build())
                              .build()));
          targetCpu = workerCpu;
          targetMem = workerMem;
          mWorkers.add(offer.getHostname());
          mTaskName = sConf.get(Constants.INTEGRATION_MESOS_TACHYON_WORKER_NAME);
        } else {
          // The resource offer cannot be used to start either master or a worker.
          driver.declineOffer(offer.getId());
          continue;
        }

        Protos.TaskID taskId =
            Protos.TaskID.newBuilder().setValue(String.valueOf(mLaunchedTasks)).build();

        System.out.println("Launching task " + taskId.getValue() + " using offer "
            + offer.getId().getValue());

        Protos.Resource.Builder portsBuilder = Protos.Resource.newBuilder()
            .setName(Constants.MESOS_RESOURCE_PORTS)
            .setType(Protos.Value.Type.RANGES);

        if (executorBuilder.getSource().equals("worker")) {
          portsBuilder.setRanges(Protos.Value.Ranges.newBuilder()
              .addRange(Protos.Value.Range.newBuilder()
                  .setBegin(sConf.getLong(Constants.WORKER_RPC_PORT))
                  .setEnd(sConf.getLong(Constants.WORKER_RPC_PORT)))
              .addRange((Protos.Value.Range.newBuilder()
                  .setBegin(sConf.getLong(Constants.WORKER_DATA_PORT))
                  .setEnd(sConf.getLong(Constants.WORKER_DATA_PORT))))
              .addRange((Protos.Value.Range.newBuilder()
                  .setBegin(sConf.getLong(Constants.WORKER_WEB_PORT))
                  .setEnd(sConf.getLong(Constants.WORKER_WEB_PORT)))));
        } else {
          portsBuilder.setRanges(Protos.Value.Ranges.newBuilder()
              .addRange(Protos.Value.Range.newBuilder()
                  .setBegin(sConf.getLong(Constants.MASTER_WEB_PORT))
                  .setEnd(sConf.getLong(Constants.MASTER_WEB_PORT)))
              .addRange((Protos.Value.Range.newBuilder()
                  .setBegin(sConf.getLong(Constants.MASTER_RPC_PORT))
                  .setEnd(sConf.getLong(Constants.MASTER_RPC_PORT)))));
        }

        Protos.TaskInfo task =
            Protos.TaskInfo
                .newBuilder()
                .setName(mTaskName)
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addResources(
                    Protos.Resource.newBuilder().setName(Constants.MESOS_RESOURCE_CPUS)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(targetCpu)))
                .addResources(
                    Protos.Resource.newBuilder().setName(Constants.MESOS_RESOURCE_MEM)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(targetMem)))
                .addResources(portsBuilder)
                .setExecutor(executorBuilder).build();

        launch.addTaskInfos(Protos.TaskInfo.newBuilder(task));
        mLaunchedTasks ++;

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
      System.out.println("Executor " + slaveId.getValue() + " was lost.");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
      String taskId = status.getTaskId().getValue();
      Protos.TaskState state = status.getState();
      System.out.printf("Task %s is in state %s%n", taskId, state);
      // TODO(jiri): Handle the case when a Tachyon master and/or worker task fails.
      // In particular, we should enable support for the fault tolerant mode of Tachyon to account
      // for Tachyon master process failures and keep track of the running number of Tachyon
      // masters.

      switch (status.getState()) {
        case TASK_FAILED: // intend to fall through
        case TASK_LOST: // intend to fall through
        case TASK_ERROR:
          if (status.getTaskId().getValue().equals(String.valueOf(mMasterTaskId))) {
            mMasterCount --;
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
  }

  private static void usage() {
    String name = TachyonFramework.class.getName();
    System.err.println("This is an implementation of a Mesos framework that is responsible for "
        + "starting\nTachyon processes. The current implementation starts a single Tachyon master "
        + "and\n n Tachyon workers (one per Mesos slave).");
    System.err.println("Usage: " + name + " <hostname>");
  }

  private static List<CommandInfo.URI> getExecutorDependencyURIList() {
    TachyonConf conf = new TachyonConf();
    String dependencyPath = conf.get(Constants.INTEGRATION_MESOS_EXECUTOR_DEPENDENCY_PATH);
    return Lists.newArrayList(
        CommandInfo.URI.newBuilder()
            .setValue(PathUtils.concatPath(dependencyPath, "tachyon.tar.gz")).setExtract(true)
            .build(),
        CommandInfo.URI.newBuilder().setValue(conf.get(Constants.INTEGRATION_MESOS_JRE_URL))
            .setExtract(true).build());
  }

  private static Protos.Credential createCredential(TachyonConf conf) {

    if (conf.get(Constants.INTEGRATION_MESOS_PRINCIPAL) == null) {
      return null;
    }

    try {
      Protos.Credential.Builder credentialBuilder = Protos.Credential.newBuilder()
          .setPrincipal(conf.get(Constants.INTEGRATION_MESOS_PRINCIPAL))
          .setSecret(ByteString.copyFrom(conf.get(Constants.INTEGRATION_MESOS_SECRET)
                  .getBytes("UTF-8")));

      return credentialBuilder.build();
    } catch (UnsupportedEncodingException ex) {
      System.err.println("Failed to encode secret when creating Credential.");
    }
    return null;
  }

  /**
   * Starts the Tachyon framework.
   *
   * @param args command-line arguments
   * @throws Exception if the Tachyon framework encounters an unrecoverable error
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      usage();
      System.exit(1);
    }
    String hostname = args[0];

    TachyonConf conf = new TachyonConf();

    // Start Mesos master. Setting the user to an empty string will prompt Mesos to set it to the
    // current user.
    Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
        .setName("tachyon").setCheckpoint(true);

    if (conf.get(Constants.INTEGRATION_MESOS_ROLE) != null) {
      frameworkInfo.setRole(conf.get(Constants.INTEGRATION_MESOS_ROLE));
    }

    if (conf.get(Constants.INTEGRATION_MESOS_USER) != null) {
      frameworkInfo.setUser(conf.get(Constants.INTEGRATION_MESOS_USER));
    }

    if (conf.get(Constants.INTEGRATION_MESOS_PRINCIPAL) != null) {
      frameworkInfo.setPrincipal(conf.get(Constants.INTEGRATION_MESOS_PRINCIPAL));
    }

    Scheduler scheduler = new TachyonScheduler();

    Protos.Credential cred = createCredential(conf);
    MesosSchedulerDriver driver = null;
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
