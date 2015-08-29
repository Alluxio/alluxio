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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.MesosSchedulerDriver;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonMaster;

public class TachyonFramework {
  static class TachyonScheduler implements Scheduler {
    private int mTasksRunning = 0;
    private int mTasksActive = 0;

    @Override
    public void disconnected(SchedulerDriver driver) {
      System.out.println("Disconnected from master.");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
      System.out.println("Error: " + message);
    }

    @Override
    public void executorLost(SchedulerDriver driver,
                             Protos.ExecutorID executorId,
                             Protos.SlaveID slaveId,
                             int status) {
      System.out.println("Executor " + executorId.getValue() + " was lost.");
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 Protos.ExecutorID executorId,
                                 Protos.SlaveID slaveId,
                                 byte[] data) {
      System.out.println("Executor: " + executorId.getValue() + ", "
          + "Slave: " + slaveId.getValue() + ", " + "Data: " + data + ".");
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
      System.out.println("Offered " + offerId.getValue() + " rescinded.");
    }

    @Override
    public void registered(SchedulerDriver driver,
                           Protos.FrameworkID frameworkId,
                           Protos.MasterInfo masterInfo) {
      System.out.println("Registered framework " + frameworkId.getValue() + " with master "
          + masterInfo.getHostname() + ":" + masterInfo.getPort() + ".");
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
      System.out.println("Registered framework with master " + masterInfo.getHostname()
          + ":" + masterInfo.getPort() + ".");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Protos.Offer> offers) {
      final double CPUS_PER_TASK = 1;
      final double MEM_PER_TASK = 128;
      int launchedTasks = 0;
      int totalTasks = 0;

      for (Protos.Offer offer : offers) {
        Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();
        double offerCpus = 0;
        double offerMem = 0;
        for (Protos.Resource resource : offer.getResourcesList()) {
          if (resource.getName().equals("cpus")) {
            offerCpus += resource.getScalar().getValue();
          } else if (resource.getName().equals("mem")) {
            offerMem += resource.getScalar().getValue();
          }
        }

        System.out.println(
            "Received offer " + offer.getId().getValue() + " with cpus: " + offerCpus
                + " and mem: " + offerMem);

        double remainingCpus = offerCpus;
        double remainingMem = offerMem;
        while (launchedTasks < totalTasks
            && remainingCpus >= CPUS_PER_TASK
            && remainingMem >= MEM_PER_TASK) {
          Protos.TaskID taskId = Protos.TaskID.newBuilder()
              .setValue(Integer.toString(launchedTasks++)).build();

          System.out.println("Launching task " + taskId.getValue()
              + " using offer " + offer.getId().getValue());

          Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
              .setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
              .setCommand(Protos.CommandInfo.newBuilder().setValue("localhost"))
              .setName("Test Tachyon Executor")
              .setSource("java_test")
              .build();

          Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
              .setName("task " + taskId.getValue())
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId())
              .addResources(Protos.Resource.newBuilder()
                  .setName("cpus")
                  .setType(Protos.Value.Type.SCALAR)
                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(CPUS_PER_TASK)))
              .addResources(Protos.Resource.newBuilder()
                  .setName("mem")
                  .setType(Protos.Value.Type.SCALAR)
                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(MEM_PER_TASK)))
              .setExecutor(Protos.ExecutorInfo.newBuilder(executor))
              .build();

          launch.addTaskInfos(Protos.TaskInfo.newBuilder(task));

          remainingCpus -= CPUS_PER_TASK;
          remainingMem -= MEM_PER_TASK;
        }

        // NOTE: We use the new API `acceptOffers` here to launch tasks.
        // The 'launchTasks' API will be deprecated.
        List<Protos.OfferID> offerIds = new ArrayList<Protos.OfferID>();
        offerIds.add(offer.getId());
        List<Protos.Offer.Operation> operations = new ArrayList<Protos.Offer.Operation>();
        Protos.Offer.Operation operation = Protos.Offer.Operation.newBuilder()
            .setType(Protos.Offer.Operation.Type.LAUNCH)
            .setLaunch(launch)
            .build();
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
          mTasksRunning ++;
          break;
        case TASK_FINISHED:
        case TASK_FAILED:
        case TASK_KILLED:
        case TASK_LOST:
          mTasksRunning --;
          break;
        default:
      }
    }
  }

  private static void usage() {
    String name = TachyonFramework.class.getName();
    System.err.println("Usage: " + name + " <hostname>");
  }

  private static TachyonConf createTachyonConfig() throws IOException {
    final int quotaUnitBytes = 100000;
    final int blockSizeByte = 1024;
    final int readBufferSizeByte = 64;

    TachyonConf conf = new TachyonConf();
    conf.set(Constants.IN_TEST_MODE, "true");
    conf.set(Constants.TACHYON_HOME,
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath());
    conf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(quotaUnitBytes));
    conf.set(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE, Integer.toString(blockSizeByte));
    conf.set(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE, Integer.toString(readBufferSizeByte));
    conf.set(Constants.MASTER_HOSTNAME, "localhost");
    conf.set(Constants.MASTER_PORT, Integer.toString(0));
    conf.set(Constants.MASTER_WEB_PORT, Integer.toString(0));

    return conf;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      usage();
      System.exit(1);
    }
    String hostname = args[0];

    // Start Tachyon master.
    TachyonConf conf = createTachyonConfig();
    LocalTachyonMaster master =
        LocalTachyonMaster.create(conf.get(Constants.TACHYON_HOME), createTachyonConfig());
    master.start();

    // Start Mesos master.
    Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("Test Tachyon Framework")
        .setPrincipal("test-tachyon-framework")
        .build();

    Scheduler scheduler = new TachyonScheduler();

    MesosSchedulerDriver driver = new MesosSchedulerDriver(scheduler, framework, hostname);

    int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    // Ensure that the Tachyon master terminates.
    master.stop();

    System.exit(status);
  }
}
