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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.status.UnavailableException;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Integration tests for AlluxioFramework. These tests assume that a Mesos cluster is
 * running locally.
 */
public final class AlluxioFrameworkIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFrameworkIntegrationTest.class);

  private static final String JDK_URL =
      "https://s3-us-west-2.amazonaws.com/alluxio-mesos/jdk-7u79-macosx-x64.tar.gz";
  private static final String JDK_PATH = "jdk1.7.0_79.jdk/Contents/Home";

  @Parameter(names = {"-m", "--mesos"}, required = true,
      description = "Address for locally-running Mesos, e.g. localhost:5050")
  private String mMesosAddress;

  @Parameter(names = {"-a", "--alluxio"},
      description = "URL of an Alluxio tarball to test. Otherwise only test local Alluxio")
  private String mAlluxioUrl;

  @Parameter(names = {"-h", "--help"}, help = true)
  private boolean mHelp = false;

  private AlluxioFrameworkIntegrationTest() {}

  private void run() throws Exception {
    checkMesosRunning();
    stopAlluxio();
    stopAlluxioFramework();
    runTests();
    LOG.info("All tests passed!");
  }

  private void runTests() throws Exception {
    LOG.info("Testing deployment with preinstalled alluxio and jdk");
    testMesosDeploy(ImmutableMap.of(
        PropertyKey.INTEGRATION_MESOS_JDK_URL, Constants.MESOS_LOCAL_INSTALL,
        PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, Constants.MESOS_LOCAL_INSTALL));
    LOG.info("Testing deployment with downloaded jdk");
    testMesosDeploy(ImmutableMap.of(
        PropertyKey.INTEGRATION_MESOS_JDK_URL, JDK_URL,
        PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, Constants.MESOS_LOCAL_INSTALL,
        PropertyKey.INTEGRATION_MESOS_JDK_PATH, JDK_PATH));
    if (mAlluxioUrl != null) {
      LOG.info("Testing deployment with downloaded Alluxio");
      testMesosDeploy(ImmutableMap.of(
          PropertyKey.INTEGRATION_MESOS_JDK_URL, Constants.MESOS_LOCAL_INSTALL,
          PropertyKey.INTEGRATION_MESOS_ALLUXIO_JAR_URL, mAlluxioUrl));
    }
  }

  private void testMesosDeploy(Map<PropertyKey, String> properties) throws Exception {
    StringBuilder alluxioJavaOpts = new StringBuilder(System.getProperty("ALLUXIO_JAVA_OPTS", ""));
    for (Entry<PropertyKey, String> entry : properties.entrySet()) {
      alluxioJavaOpts
          .append(String.format(" -D%s=%s", entry.getKey().toString(), entry.getValue()));
    }
    Map<String, String> env = ImmutableMap.of("ALLUXIO_JAVA_OPTS", alluxioJavaOpts.toString());
    try {
      startAlluxioFramework(env);
      LOG.info("Launched Alluxio cluster, waiting for worker to register with master");
      String masterHostName = NetworkAddressUtils.getLocalHostName();
      int masterPort = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
      InetSocketAddress masterAddress = new InetSocketAddress(masterHostName, masterPort);
      try (final BlockMasterClient client = BlockMasterClient.Factory.create(masterAddress)) {
        CommonUtils.waitFor("Alluxio worker to register with master",
            new Function<Void, Boolean>() {
              @Override
              public Boolean apply(Void input) {
                try {
                  try {
                    return !client.getWorkerInfoList().isEmpty();
                  } catch (UnavailableException e) {
                    // block master isn't up yet, keep waiting
                    return false;
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }, WaitForOptions.defaults().setTimeoutMs(15 * Constants.MINUTE_MS));
      }
      LOG.info("Worker registered");
      basicAlluxioTests();
    } finally {
      stopAlluxioFramework();
    }
  }

  private void startAlluxioFramework(Map<String, String> extraEnv) {
    String startScript = PathUtils.concatPath(Configuration.get(PropertyKey.HOME),
        "integration", "mesos", "bin", "alluxio-mesos-start.sh");
    ProcessBuilder pb = new ProcessBuilder(startScript, mMesosAddress);
    Map<String, String> env = pb.environment();
    env.putAll(extraEnv);
    try {
      pb.start().waitFor();
    } catch (Exception e) {
      LOG.info("Failed to launch Alluxio on Mesos. Note that this test requires that "
          + "Mesos is currently running.");
      throw new RuntimeException(e);
    }
  }

  private static void basicAlluxioTests() throws Exception {
    LOG.info("Running tests");
    FileSystem fs = FileSystem.Factory.get();
    int listSize = fs.listStatus(new AlluxioURI("/")).size();
    if (listSize != 1) {
      throw new RuntimeException("Expected 1 path to exist at the root, but found " + listSize);
    }
    FileOutStream outStream = fs.createFile(new AlluxioURI("/test"));
    outStream.write("abc".getBytes());
    outStream.close();
    FileInStream inStream = fs.openFile(new AlluxioURI("/test"));
    String result = IOUtils.toString(inStream);
    if (!result.equals("abc")) {
      throw new RuntimeException("Expected abc but got " + result);
    }
    LOG.info("Tests passed");
  }

  private static void checkMesosRunning() throws Exception {
    for (String processName : new String[]{"mesos-master", "mesos-slave"}) {
      if (!processExists(processName)) {
        throw new IllegalStateException(String.format(
            "Couldn't find local '%s' process. Mesos must be running locally to use this test",
            processName));
      }
    }
  }

  private static boolean processExists(String processName) throws Exception {
    Process ps = Runtime.getRuntime().exec(new String[] {"ps", "ax"});
    InputStream psOutput = ps.getInputStream();

    Process processGrep = Runtime.getRuntime().exec(new String[] {"grep", processName});
    OutputStream processGrepInput = processGrep.getOutputStream();
    IOUtils.copy(psOutput, processGrepInput);
    InputStream processGrepOutput = processGrep.getInputStream();
    processGrepInput.close();

    // Filter out the grep process itself.
    Process filterGrep = Runtime.getRuntime().exec(new String[] {"grep", "-v", "grep"});
    OutputStream filterGrepInput = filterGrep.getOutputStream();
    IOUtils.copy(processGrepOutput, filterGrepInput);
    filterGrepInput.close();

    return IOUtils.readLines(filterGrep.getInputStream()).size() >= 1;
  }

  private static void stopAlluxioFramework() throws Exception {
    String stopScript = PathUtils.concatPath(Configuration.get(PropertyKey.HOME),
        "integration", "mesos", "bin", "alluxio-mesos-stop.sh");
    ProcessBuilder pb = new ProcessBuilder(stopScript);
    pb.start().waitFor();
    // Wait for Mesos to unregister and shut down the Alluxio Framework.
    CommonUtils.sleepMs(5000);
  }

  private static void stopAlluxio() throws Exception {
    String stopScript = PathUtils.concatPath(Configuration.get(PropertyKey.HOME),
        "bin", "alluxio-stop.sh");
    ProcessBuilder pb = new ProcessBuilder(stopScript, "all");
    pb.start().waitFor();
  }

  /**
   * @param args arguments
   */
  public static void main(String[] args) throws Exception {
    AlluxioFrameworkIntegrationTest test = new AlluxioFrameworkIntegrationTest();
    JCommander jc = new JCommander(test);
    jc.setProgramName(AlluxioFrameworkIntegrationTest.class.getName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      jc.usage();
      System.exit(1);
    }
    if (test.mHelp) {
      jc.usage();
    } else {
      test.run();
    }
  }
}
