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
import alluxio.PropertyKey;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.protobuf.ByteString;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

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
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFramework.class);

  @Parameter(names = {"-m", "--mesos"}, description = "Mesos master location, e.g. localhost:5050")
  private String mMesosMaster;

  @Parameter(names = {"-a", "--alluxio-master"},
      description = "Host to launch the Alluxio Master on")
  private String mAlluxioMasterHostname;

  /**
   * Creates a new {@link AlluxioFramework}.
   */
  public AlluxioFramework() {}

  /**
   * Runs the mesos framework.
   */
  public void run() {
    Protos.FrameworkInfo.Builder frameworkInfo = Protos.FrameworkInfo.newBuilder()
        .setName("alluxio").setCheckpoint(true);

    if (Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_ROLE)) {
      frameworkInfo.setRole(Configuration.get(PropertyKey.INTEGRATION_MESOS_ROLE));
    }
    // Setting the user to an empty string will prompt Mesos to set it to the current user.
    if (Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_USER)) {
      frameworkInfo.setUser(Configuration.get(PropertyKey.INTEGRATION_MESOS_USER));
    }

    if (Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_PRINCIPAL)) {
      frameworkInfo.setPrincipal(Configuration.get(PropertyKey.INTEGRATION_MESOS_PRINCIPAL));
    }

    Scheduler scheduler = new AlluxioScheduler(mAlluxioMasterHostname);

    Protos.Credential cred = createCredential();
    MesosSchedulerDriver driver;
    if (cred == null) {
      driver = new MesosSchedulerDriver(scheduler, frameworkInfo.build(), mMesosMaster);
    } else {
      driver = new MesosSchedulerDriver(scheduler, frameworkInfo.build(), mMesosMaster, cred);
    }

    int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

    System.exit(status);
  }

  private static Protos.Credential createCredential() {
    if (!(Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_PRINCIPAL)
        && Configuration.containsKey(PropertyKey.INTEGRATION_MESOS_SECRET))) {
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
   */
  public static void main(String[] args) throws Exception {
    AlluxioFramework framework = new AlluxioFramework();
    JCommander jc = new JCommander(framework);
    try {
      jc.parse(args);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      jc.usage();
      System.exit(1);
    }
    framework.run();
  }
}
