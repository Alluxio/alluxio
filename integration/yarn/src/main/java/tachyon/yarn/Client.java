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

package tachyon.yarn;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;
import tachyon.yarn.YarnUtils.YarnContainerType;

/**
 * The client to submit the application to run Tachyon to YARN ResourceManager.
 *
 * <p>
 * Launch Tachyon on YARN:
 * </p>
 * {@code
 * $ yarn jar tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar tachyon.yarn.Client \
 *     -num_workers NumTachyonWorkers \
 *     -master_address MasterAddress \
 *     -resource_path ResourcePath
 * }
 *
 * <p>
 * Get help and a full list of options:
 * </p>
 * {@code
 * $ yarn jar tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar tachyon.yarn.Client -help
 * }
 */
public final class Client {
  /** Yarn client to talk to resource manager. */
  private YarnClient mYarnClient;
  /** Yarn configuration. */
  private YarnConfiguration mYarnConf = new YarnConfiguration();
  /** Container context to launch application master. */
  private ContainerLaunchContext mAmContainer;
  /** ApplicationMaster specific info to register a new Application. */
  private ApplicationSubmissionContext mAppContext;
  /** Application name. */
  private String mAppName;
  /** ApplicationMaster priority. */
  private int mAmPriority;
  /** Queue for ApplicationMaster. */
  private String mAmQueue;
  /** Amount of memory to request for running the ApplicationMaster. */
  private int mAmMemoryInMB;
  /** Number of virtual cores to request for running the ApplicationMaster. */
  private int mAmVCores;
  /** ApplicationMaster jar file on HDFS. */
  private String mResourcePath;
  /** Number of Tachyon workers. */
  private int mNumWorkers;
  /** Address to run Tachyon master. */
  private String mMasterAddress;
  /** Maximum number of workers to allow on a single host */
  private int mMaxWorkersPerHost;
  /** Id of the application */
  private ApplicationId mAppId;
  /** Command line options */
  private Options mOptions;

  public Client() {
    mOptions = new Options();
    mOptions.addOption("appname", true, "Application Name. Default 'Tachyon'");
    mOptions.addOption("priority", true, "Application Priority. Default 0");
    mOptions.addOption("queue", true,
        "RM Queue in which this application is to be submitted. Default 'default'");
    mOptions.addOption("am_memory", true,
        "Amount of memory in MB to request to run ApplicationMaster. Default 256");
    mOptions.addOption("am_vcores", true,
        "Amount of virtual cores to request to run ApplicationMaster. Default 1");
    mOptions.addOption("resource_path", true,
        "(Required) HDFS path containing the Application Master");
    mOptions.addOption("tachyon_home", true,
        "(Required) Path of the home dir of Tachyon deployment on YARN slave machines");
    mOptions.addOption("master_address", true, "(Required) Address to run Tachyon master");
    mOptions.addOption("help", false, "Print usage");
    mOptions.addOption("num_workers", true, "Number of Tachyon workers to launch. Default 1");
  }

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      System.out.println("Initializing Client");
      if (!client.parseArgs(args)) {
        System.out.println("Cannot parse commandline: " + Arrays.toString(args));
        System.exit(0);
      }
      System.out.println("Starting Client");
      result = client.run();
    } catch (Exception e) {
      System.err.println("Error running Client " + e);
      System.exit(1);
    }
    if (result) {
      System.out.println("Application completed successfully");
      System.exit(0);
    }
    System.err.println("Application failed to complete");
    System.exit(2);
  }

  /**
   * Main run function for the client
   *
   * @return true if application completed successfully
   * @throws IOException if errors occur from ResourceManager
   * @throws YarnException if errors occur from ResourceManager
   */
  public boolean run() throws IOException, YarnException {
    submitApplication();
    return monitorApplication();
  }

  /**
   * Helper function to print out usage.
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", mOptions);
  }

  /**
   * Parses command line options.
   *
   * @param args Parsed command line options
   * @return Whether the parseArgs was successful to run the client
   * @throws ParseException if an error occurs when parsing the argument
   */
  private boolean parseArgs(String[] args) throws ParseException {
    Preconditions.checkArgument(args.length > 0, "No args specified for client to initialize");
    CommandLine cliParser = new GnuParser().parse(mOptions, args);

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }
    if (!cliParser.hasOption("resource_path")) {
      System.out.println("Required to specify resource_path");
      printUsage();
      return false;
    }

    mResourcePath = cliParser.getOptionValue("resource_path");
    mMasterAddress = cliParser.getOptionValue("master_address");
    mAppName = cliParser.getOptionValue("appname", "Tachyon");
    mAmPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    mAmQueue = cliParser.getOptionValue("queue", "default");
    mAmMemoryInMB = Integer.parseInt(cliParser.getOptionValue("am_memory", "256"));
    mAmVCores = Integer.parseInt(cliParser.getOptionValue("am_vcores", "1"));
    mNumWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));
    mMaxWorkersPerHost =
        new TachyonConf().getInt(Constants.INTEGRATION_YARN_WORKERS_PER_HOST_MAX);

    Preconditions.checkArgument(mAmMemoryInMB > 0,
        "Invalid memory specified for application master, " + "exiting. Specified memory="
            + mAmMemoryInMB);
    Preconditions.checkArgument(mAmVCores > 0,
        "Invalid virtual cores specified for application master, exiting."
            + " Specified virtual cores=" + mAmVCores);
    return true;
  }

  /**
   * Submits an application to the ResourceManager to run ApplicationMaster.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for creating
   * applications and setting up the application submission context. This was not available in the
   * alpha API.
   */
  private void submitApplication() throws YarnException, IOException {
    // TODO(binfan): setup credential

    // Initialize a YarnClient
    mYarnClient = YarnClient.createYarnClient();
    mYarnClient.init(mYarnConf);
    mYarnClient.start();

    // Create an application, get and check the information about the cluster
    YarnClientApplication app = mYarnClient.createApplication();
    // Get a response of this application, containing information of the cluster
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    // Check if the cluster has enough resource to launch the ApplicationMaster
    checkClusterResource(appResponse);

    // Check that there are enough hosts in the cluster to support the desired number of workers
    checkNodesAvailable();

    // Set up the container launch context for the application master
    mAmContainer = Records.newRecord(ContainerLaunchContext.class);
    setupContainerLaunchContext();

    // Finally, set-up ApplicationSubmissionContext for the application
    mAppContext = app.getApplicationSubmissionContext();
    setupApplicationSubmissionContext();

    // Submit the application to the applications manager.
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    mAppId = mAppContext.getApplicationId();
    System.out.println("Submitting application of id " + mAppId + " to ResourceManager");
    mYarnClient.submitApplication(mAppContext);
  }

  // Checks if the cluster has enough resource to launch application master
  private void checkClusterResource(GetNewApplicationResponse appResponse) {
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();

    Preconditions.checkArgument(mAmMemoryInMB <= maxMem,
        "ApplicationMaster memory specified above max threshold of cluster, specified="
            + mAmMemoryInMB + ", max=" + maxMem);
    Preconditions.checkArgument(mAmVCores <= maxVCores,
        "ApplicationMaster virtual cores specified above max threshold of cluster, specified="
            + mAmVCores + ", max=" + maxVCores);
  }

  // Checks that there are enough nodes in the cluster to run the desired number of workers
  private void checkNodesAvailable() throws YarnException, IOException {
    Set<String> hosts = YarnUtils.getNodeHosts(mYarnClient);
    Preconditions.checkArgument(mNumWorkers <= hosts.size() * mMaxWorkersPerHost,
        "Not enough nodes in cluster to support specified number of workers, " + String.format(
            "specified=%s, but there are only %d usable hosts and %d workers allowed per host: %s",
            mNumWorkers, hosts.size(), mMaxWorkersPerHost, hosts));
  }

  private void setupContainerLaunchContext() throws IOException {
    Map<String, String> applicationMasterArgs = ImmutableMap.<String, String>of(
        "-num_workers", Integer.toString(mNumWorkers),
        "-master_address", mMasterAddress,
        "-resource_path", mResourcePath);

    final String amCommand =
        YarnUtils.buildCommand(YarnContainerType.APPLICATION_MASTER, applicationMasterArgs);

    System.out.println("ApplicationMaster command: " + amCommand);
    mAmContainer.setCommands(Collections.singletonList(amCommand));

    // Setup local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put("tachyon.tar.gz",
        YarnUtils.createLocalResourceOfFile(mYarnConf, mResourcePath + "/tachyon.tar.gz"));
    localResources.put("tachyon-yarn-setup.sh",
        YarnUtils.createLocalResourceOfFile(mYarnConf, mResourcePath + "/tachyon-yarn-setup.sh"));
    localResources.put("tachyon.jar",
        YarnUtils.createLocalResourceOfFile(mYarnConf, mResourcePath + "/tachyon.jar"));
    mAmContainer.setLocalResources(localResources);

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    mAmContainer.setEnvironment(appMasterEnv);
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    String classpath = ApplicationConstants.Environment.CLASSPATH.name();
    for (String path : mYarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, classpath, path.trim(),
          ApplicationConstants.CLASS_PATH_SEPARATOR);
    }
    Apps.addToEnvironment(appMasterEnv, classpath, PathUtils.concatPath(Environment.PWD.$(), "*"),
        ApplicationConstants.CLASS_PATH_SEPARATOR);

    appMasterEnv.put("TACHYON_HOME", ApplicationConstants.Environment.PWD.$());
  }

  /**
   * Sets up the application submission context.
   */
  private void setupApplicationSubmissionContext() {
    // set the application name
    mAppContext.setApplicationName(mAppName);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and vcores requirements
    Resource capability = Resource.newInstance(mAmMemoryInMB, mAmVCores);
    mAppContext.setResource(capability);

    // Set the queue to which this application is to be submitted in the RM
    mAppContext.setQueue(mAmQueue);

    // Set the AM container spec
    mAppContext.setAMContainerSpec(mAmContainer);

    // Set the priority for the application master
    mAppContext.setPriority(Priority.newInstance(mAmPriority));
  }

  /**
   * Monitors the submitted application for completion.
   *
   * @return true if application completed successfully
   * @throws YarnException if errors occur when obtaining application report from ResourceManager
   * @throws IOException if errors occur when obtaining application report from ResourceManager
   */
  private boolean monitorApplication() throws YarnException, IOException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    while (true) {
      // Check app status every 10 second.
      CommonUtils.sleepMs(10 * Constants.SECOND_MS);
      Date date = new Date();
      String timeDateStr = dateFormat.format(date);

      // Get application report for the appId we are interested in
      ApplicationReport report = mYarnClient.getApplicationReport(mAppId);

      System.out.println(timeDateStr + " Got application report from ASM for appId="
          + mAppId.getId() + ", clientToAMToken=" + report.getClientToAMToken()
          + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime() + ", yarnAppState="
          + report.getYarnApplicationState().toString() + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
          + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          System.out.println("Application has completed successfully. Breaking monitoring loop");
          return true;
        }
        System.out.println("Application did finished unsuccessfully. YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
        return false;
      }

      if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
        System.out.println("Application did not finish. YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
        return false;
      }
    }
  }

}
