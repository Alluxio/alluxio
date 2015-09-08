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

import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.MesosSchedulerDriver;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.io.PathUtils;

public class TachyonFramework {
  static class TachyonScheduler implements Scheduler {
    private boolean mMasterLaunched = false;

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
          + slaveId.getValue() + ", " + "Data: " + data + ".");
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
      TachyonConf conf = new TachyonConf();
      double masterCpus = conf.get(Constants.MASTER_CPU, Constants.DEFAULT_MASTER_CPU);
      double masterMemMB = conf.get(Constants.MASTER_MEM_MB, Constants.DEFAULT_MASTER_MEM_MB);
      String tachyonHome = conf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
      int launchedTasks = 0;

      for (Protos.Offer offer : offers) {
        Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();
        double offerCpus = 0;
        double offerMemMb = 0;
        for (Protos.Resource resource : offer.getResourcesList()) {
          if (resource.getName().equals("cpus")) {
            offerCpus += resource.getScalar().getValue();
          } else if (resource.getName().equals("mem")) {
            offerMemMb += resource.getScalar().getValue();
          }
        }

        System.out.println("Received offer " + offer.getId().getValue() + " with cpus: "
            + offerCpus + " and mem: " + offerMemMb);

        double remainingCpus = offerCpus;
        double remainingMem = offerMemMb;
        while (remainingCpus > 0 && remainingMem > 0) {
          Protos.TaskID taskId =
              Protos.TaskID.newBuilder().setValue(Integer.toString(launchedTasks ++)).build();

          System.out.println("Launching task " + taskId.getValue() + " using offer "
              + offer.getId().getValue());

          Protos.ExecutorInfo.Builder executorBuilder = Protos.ExecutorInfo.newBuilder();
          double targetCpus = 0;
          double targetMem = 0;
          if (!mMasterLaunched && remainingCpus >= masterCpus && remainingMem >= masterMemMB) {
            executorBuilder
                .setName("Tachyon Master Executor")
                .setSource("master")
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("master"))
                .setCommand(
                    Protos.CommandInfo.newBuilder().setValue(
                        PathUtils.concatPath(tachyonHome, "mesos", "bin", "tachyon-master.sh")));
            targetCpus = masterCpus;
            targetMem = masterMemMB;
            mMasterLaunched = true;
          } else {
            executorBuilder
                .setName("Tachyon Worker Executor")
                .setSource("worker")
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("worker"))
                .setCommand(
                    Protos.CommandInfo.newBuilder().setValue(
                        PathUtils.concatPath(tachyonHome, "mesos", "bin", "tachyon-worker.sh")));
            targetCpus = remainingCpus;
            targetMem = remainingMem;
          }
          Protos.TaskInfo task =
              Protos.TaskInfo
                  .newBuilder()
                  .setName("task " + taskId.getValue())
                  .setTaskId(taskId)
                  .setSlaveId(offer.getSlaveId())
                  .addResources(
                      Protos.Resource.newBuilder().setName("cpus")
                          .setType(Protos.Value.Type.SCALAR)
                          .setScalar(Protos.Value.Scalar.newBuilder().setValue(targetCpus)))
                  .addResources(
                      Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR)
                          .setScalar(Protos.Value.Scalar.newBuilder().setValue(targetMem)))
                  .setExecutor(executorBuilder).build();

          launch.addTaskInfos(Protos.TaskInfo.newBuilder(task));

          remainingCpus -= targetCpus;
          remainingMem -= targetMem;
        }

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
      System.out.println("Executor " + slaveId.getValue() + " was lost.");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
      String taskId = status.getTaskId().getValue();
      Protos.TaskState state = status.getState();
      System.out.printf("Task %s is in state %s\n", taskId, state);
      switch (state) {
        case TASK_RUNNING:
        case TASK_FINISHED:
        case TASK_FAILED:
        case TASK_KILLED:
        case TASK_LOST:
        default:
      }
    }
  }

  private static void usage() {
    String name = TachyonFramework.class.getName();
    System.err.println("Usage: " + name + " <hostname>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      usage();
      System.exit(1);
    }
    String hostname = args[0];

    // Start Mesos master. Have Mesos fill in the current user.
    Protos.FrameworkInfo framework =
        Protos.FrameworkInfo.newBuilder().setUser("").setName("Test Tachyon Framework")
            .setPrincipal("test-tachyon-framework").build();

    Scheduler scheduler = new TachyonScheduler();

    MesosSchedulerDriver driver = new MesosSchedulerDriver(scheduler, framework, hostname);

    int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    System.exit(status);
  }
}
