/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tachyon.mesos;

import java.io.File;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class TestFramework {
  static class TestScheduler implements Scheduler {
    public TestScheduler(boolean implicitAcknowledgements,
                         ExecutorInfo executor) {
      this(implicitAcknowledgements, executor, 5);
    }

    public TestScheduler(boolean implicitAcknowledgements,
                         ExecutorInfo executor,
                         int totalTasks) {
      this.implicitAcknowledgements = implicitAcknowledgements;
      this.executor = executor;
      this.totalTasks = totalTasks;
    }

    @Override
    public void registered(SchedulerDriver driver,
                           FrameworkID frameworkId,
                           MasterInfo masterInfo) {
      System.out.println("Registered! ID = " + frameworkId.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {}

    @Override
    public void disconnected(SchedulerDriver driver) {}

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Offer> offers) {
      double CPUS_PER_TASK = 1;
      double MEM_PER_TASK = 128;

      for (Offer offer : offers) {
        Offer.Operation.Launch.Builder launch = Offer.Operation.Launch.newBuilder();
        double offerCpus = 0;
        double offerMem = 0;
        for (Resource resource : offer.getResourcesList()) {
          if (resource.getName().equals("cpus")) {
            offerCpus += resource.getScalar().getValue();
          } else if (resource.getName().equals("mem")) {
            offerMem += resource.getScalar().getValue();
          }
        }

        System.out.println(
            "Received offer " + offer.getId().getValue() + " with cpus: " + offerCpus +
                " and mem: " + offerMem);

        double remainingCpus = offerCpus;
        double remainingMem = offerMem;
        while (launchedTasks < totalTasks &&
            remainingCpus >= CPUS_PER_TASK &&
            remainingMem >= MEM_PER_TASK) {
          TaskID taskId = TaskID.newBuilder()
              .setValue(Integer.toString(launchedTasks++)).build();

          System.out.println("Launching task " + taskId.getValue() +
              " using offer " + offer.getId().getValue());

          TaskInfo task = TaskInfo.newBuilder()
              .setName("task " + taskId.getValue())
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId())
              .addResources(Resource.newBuilder()
                  .setName("cpus")
                  .setType(Value.Type.SCALAR)
                  .setScalar(Value.Scalar.newBuilder().setValue(CPUS_PER_TASK)))
              .addResources(Resource.newBuilder()
                  .setName("mem")
                  .setType(Value.Type.SCALAR)
                  .setScalar(Value.Scalar.newBuilder().setValue(MEM_PER_TASK)))
              .setExecutor(ExecutorInfo.newBuilder(executor))
              .build();

          launch.addTaskInfos(TaskInfo.newBuilder(task));

          remainingCpus -= CPUS_PER_TASK;
          remainingMem -= MEM_PER_TASK;
        }

        // NOTE: We use the new API `acceptOffers` here to launch tasks. The
        // 'launchTasks' API will be deprecated.
        List<OfferID> offerIds = new ArrayList<OfferID>();
        offerIds.add(offer.getId());

        List<Offer.Operation> operations = new ArrayList<Offer.Operation>();

        Offer.Operation operation = Offer.Operation.newBuilder()
            .setType(Offer.Operation.Type.LAUNCH)
            .setLaunch(launch)
            .build();

        operations.add(operation);

        Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();

        driver.acceptOffers(offerIds, operations, filters);
      }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
      System.out.println("Status update: task " + status.getTaskId().getValue() +
          " is in state " + status.getState().getValueDescriptor().getName());
      if (status.getState() == TaskState.TASK_FINISHED) {
        finishedTasks++;
        System.out.println("Finished tasks: " + finishedTasks);
        if (finishedTasks == totalTasks) {
          driver.stop();
        }
      }

      if (status.getState() == TaskState.TASK_LOST ||
          status.getState() == TaskState.TASK_KILLED ||
          status.getState() == TaskState.TASK_FAILED) {
        System.err.println("Aborting because task " + status.getTaskId().getValue() +
            " is in unexpected state " +
            status.getState().getValueDescriptor().getName() +
            " with reason '" +
            status.getReason().getValueDescriptor().getName() + "'" +
            " from source '" +
            status.getSource().getValueDescriptor().getName() + "'" +
            " with message '" + status.getMessage() + "'");
        driver.abort();
      }

      if (!implicitAcknowledgements) {
        driver.acknowledgeStatusUpdate(status);
      }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 ExecutorID executorId,
                                 SlaveID slaveId,
                                 byte[] data) {}

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

    @Override
    public void executorLost(SchedulerDriver driver,
                             ExecutorID executorId,
                             SlaveID slaveId,
                             int status) {}

    public void error(SchedulerDriver driver, String message) {
      System.out.println("Error: " + message);
    }

    private final boolean implicitAcknowledgements;
    private final ExecutorInfo executor;
    private final int totalTasks;
    private int launchedTasks = 0;
    private int finishedTasks = 0;
  }

  private static void usage() {
    String name = TestFramework.class.getName();
    System.err.println("Usage: " + name + " master <tasks>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      usage();
      System.exit(1);
    }

    String uri = new File("/Users/jsimsa/Projects/mesos-0.23.0/build/src/examples/java/test-executor").getCanonicalPath();

    ExecutorInfo executor = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue("default"))
        .setCommand(CommandInfo.newBuilder().setValue(uri))
        .setName("Test Executor (Java)")
        .setSource("java_test")
        .build();

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("Test Framework (Java)");

    // TODO(vinod): Make checkpointing the default when it is default
    // on the slave.
    if (System.getenv("MESOS_CHECKPOINT") != null) {
      System.out.println("Enabling checkpoint for the framework");
      frameworkBuilder.setCheckpoint(true);
    }

    boolean implicitAcknowledgements = true;

    if (System.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS") != null) {
      System.out.println("Enabling explicit acknowledgements for status updates");
      implicitAcknowledgements = false;
    }

    Scheduler scheduler = args.length == 1
        ? new TestScheduler(implicitAcknowledgements, executor)
        : new TestScheduler(implicitAcknowledgements, executor, Integer.parseInt(args[1]));

    MesosSchedulerDriver driver = null;
    if (System.getenv("MESOS_AUTHENTICATE") != null) {
      System.out.println("Enabling authentication for the framework");

      if (System.getenv("DEFAULT_PRINCIPAL") == null) {
        System.err.println("Expecting authentication principal in the environment");
        System.exit(1);
      }

      Credential.Builder credentialBuilder = Credential.newBuilder()
          .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

      if (System.getenv("DEFAULT_SECRET") != null) {
        credentialBuilder.setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()));
      }

      frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

      driver = new MesosSchedulerDriver(
          scheduler,
          frameworkBuilder.build(),
          args[0],
          implicitAcknowledgements,
          credentialBuilder.build());
    } else {
      frameworkBuilder.setPrincipal("test-framework-java");

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], implicitAcknowledgements);
    }

    int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    System.exit(status);
  }
}
