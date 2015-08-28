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
      System.out.println("Executor: " + executorId.getValue() + ", " +
            "Slave: " + slaveId.getValue() + ", " + "Data: " + data + ".");
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
      for (Protos.Offer offer : offers) {
        System.out.println("Resource offer: " + offer.toString());
        // TODO create Tachyon task
      }
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
      System.out.println("Executor " + slaveId.getValue() + " was lost.");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
      System.out.println("Status update: " + status.getMessage());
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
