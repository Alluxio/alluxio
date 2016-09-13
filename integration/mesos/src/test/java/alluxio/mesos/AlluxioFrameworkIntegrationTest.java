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

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.ConnectionFailedException;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Integration tests for {@link AlluxioFramework}. These tests assume that a mesos cluster is
 * running locally and take
 */
public class AlluxioFrameworkIntegrationTest {
  private static final String JDK_URL =
      "https://s3-us-west-2.amazonaws.com/alluxio-mesos/jdk-7u79-macosx-x64.tar.gz";
  private static final String JDK_PATH = "jdk1.7.0_79.jdk/Contents/Home";
  private static final String ALLUXIO_URL =
      "https://s3-us-west-2.amazonaws.com/alluxio-mesos/alluxio-1.3.0-SNAPSHOT-bin.tar.gz";

  private static void runTests(String mesosAddress) throws Exception {
    System.out.println("Testing preinstalled alluxio + java deployment");
    testMesosDeploy(mesosAddress, ImmutableMap.of(
        PropertyKey.INTEGRATION_MESOS_JRE_URL, "PREINSTALLED",
        PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, "PREINSTALLED"));
    System.out.println("Testing downloaded alluxio + java deployment");
    testMesosDeploy(mesosAddress,
        ImmutableMap.of(PropertyKey.INTEGRATION_MESOS_JRE_URL, JDK_URL,
            PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, ALLUXIO_URL,
            PropertyKey.INTEGRATION_MESOS_JRE_PATH, JDK_PATH));
    System.out.println("Testing downloaded alluxio + preinstalled java deployment");
    testMesosDeploy(mesosAddress,
        ImmutableMap.of(PropertyKey.INTEGRATION_MESOS_JRE_URL, JDK_URL,
            PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, "PREINSTALLED",
            PropertyKey.INTEGRATION_MESOS_JRE_PATH, JDK_PATH));
    System.out.println("Testing downloaded java + preinstalled alluxio deployment");
    testMesosDeploy(mesosAddress,
        ImmutableMap.of(PropertyKey.INTEGRATION_MESOS_JRE_URL, "PREINSTALLED",
            PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, "PREINSTALLED"));
  }

  private static void testMesosDeploy(String mesosAddress, Map<PropertyKey, String> properties)
      throws Exception {
    StringBuilder alluxioJavaOpts = new StringBuilder(System.getProperty("ALLUXIO_JAVA_OPTS", ""));
    for (Entry<PropertyKey, String> entry : properties.entrySet()) {
      alluxioJavaOpts
          .append(String.format(" -D%s=%s", entry.getKey().toString(), entry.getValue()));
    }
    Map<String, String> env = ImmutableMap.of("ALLUXIO_JAVA_OPTS", alluxioJavaOpts.toString());
    try {
      startAlluxioFramework(mesosAddress, env);
      System.out.println("Launched Alluxio cluster, waiting for worker to register with master");
      try (final BlockMasterClient client =
          new RetryHandlingBlockMasterClient(ClientContext.getMasterAddress())) {
        CommonTestUtils.waitFor("Alluxio worker to register with master",
            new Function<Void, Boolean>() {
              @Override
          public Boolean apply(Void input) {
            try {
              try {
                return !client.getWorkerInfoList().isEmpty();
              } catch (ConnectionFailedException e) {
                // block master isn't up yet, keep waiting
                return false;
              }
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }, 300 * Constants.SECOND_MS);
      }
      System.out.println("Worker registered");
      basicAlluxioTests();
    } finally {
      stopAlluxioFramework();
    }
  }

  private static void startAlluxioFramework(String mesosAddress, Map<String, String> extraEnv) {
    String startScript = PathUtils.concatPath(Configuration.get(PropertyKey.HOME),
        "integration", "bin", "alluxio-mesos.sh");
    ProcessBuilder pb = new ProcessBuilder(startScript, mesosAddress, "-w");
    Map<String, String> env = pb.environment();
    env.putAll(extraEnv);
    try {
      pb.start().waitFor();
    } catch (Exception e) {
      System.out.println("Failed to launch Alluxio on Mesos. Note that this test requires that "
          + "Mesos is currently running.");
      throw Throwables.propagate(e);
    }
  }

  private static void basicAlluxioTests() throws Exception {
    System.out.println("Running tests");
    FileSystem fs = FileSystem.Factory.get();
    assertEquals(1, fs.listStatus(new AlluxioURI("/")).size());
    FileOutStream outStream = fs.createFile(new AlluxioURI("/test"));
    outStream.write("abc".getBytes());
    outStream.close();
    FileInStream inStream = fs.openFile(new AlluxioURI("/test"));
    assertEquals("abc", IOUtils.toString(inStream));
    System.out.println("Tests passed");
  }

  private static void stopAlluxioFramework() throws Exception {
    String stopScript = PathUtils.concatPath(Configuration.get(PropertyKey.HOME),
        "integration", "bin", "stop-mesos.sh");
    ProcessBuilder pb = new ProcessBuilder(stopScript);
    pb.start().waitFor();
    // Wait for mesos to unregister and shut down the Alluxio Framework.
    CommonUtils.sleepMs(10000);
  }

  private static void stopAlluxio() throws Exception {
    String stopScript = PathUtils.concatPath(Configuration.get(PropertyKey.HOME),
        "bin", "alluxio-stop.sh");
    ProcessBuilder pb = new ProcessBuilder(stopScript, "all");
    pb.start().waitFor();
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("Usage: AlluxioFrameworkIntegrationTest MESOS_MASTER_ADDRESS");
      System.out.println("MESOS_MASTER_ADDRESS is of the form 'mesosMasterHostname:5050'");
      System.exit(1);
    }
    String mesosAddress = args[0];
    stopAlluxio();
    stopAlluxioFramework();
    runTests(mesosAddress);
    System.out.println("All tests passed!");
  }
}
