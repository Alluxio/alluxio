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

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * Client to submit Tachyon application to Hadoop YARN ResourceManager (RM).
 *
 * <p>
 * Launch Tachyon on YARN:
 * </p>
 * <code>
 * $ yarn jar tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar tachyon.yarn.Client -jar
 * hdfs://HDFSMaster:port/path/to/tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar
 * -num_workers NumTachyonWorkers
 * </code>
 *
 * <p>
 * Check help:
 * </p>
 * <code>
 * $ yarn jar tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar tachyon.yarn.Client -help
 * </code>
 */
public final class Client {
  /** Main class to invoke application master */
  private final String mAppMasterMainClass = ApplicationMaster.class.getName();
  /** Yarn client to talk to resource manager */
  private YarnClient mYarnClient;
  /** Yarn configuration */
  private YarnConfiguration mYarnConf = new YarnConfiguration();
  /** Container context to launch application master */
  private ContainerLaunchContext mAmContainer;
  /** Application master specific info to register a new Application with RM/ASM */
  private ApplicationSubmissionContext mAppContext;
  /** Application name */
  private String mAppName;
  /** Application master priority */
  private int mAmPriority;
  /** Queue for App master */
  private String mAmQueue;
  /** Amount of memory resource to request for to run the App Master */
  private int mAmMemory;
  /** Number of virtual core resource to request for to run the App Master */
  private int mAmVCores;
  /** Application master jar file on HDFS */
  private String mHDFSAppMasterJar;
  /** Number of tachyon workers. */
  private int mNumWorkers;
  /** Tachyon home path on YARN containers. */
  private String mTachyonHome;
  /** Address to run Tachyon master. */
  private String mMasterAddress;
  /** Id of the application */
  private ApplicationId mAppId;
  /** Command line options */
  private Options mOptions;

  public Client() {
    mOptions = new Options();
    mOptions.addOption("appname", true, "Application Name. Default value Tachyon");
    mOptions.addOption("priority", true, "Application Priority. Default 0");
    mOptions.addOption("queue", true, "RM Queue in which this application is to be submitted");
    mOptions.addOption("user", true, "User to run the application as");
    mOptions.addOption("master_memory", true,
        "Amount of memory in MB to be requested to run the application master");
    mOptions.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the "
        + "application master");
    mOptions.addOption("jar", true, "Jar file containing the application master");
    mOptions.addOption("tachyon_home", true, "Path to Tachyon home dir on YARN slave machines");
    mOptions.addOption("master_address", true, "Address to run Tachyon master");
    mOptions.addOption("help", false, "Print usage");
    mOptions.addOption("num_workers", true, "Number of tachyon workers to launch");
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
        System.exit(0);
      }
      System.out.println("Starting Client");
      result = client.run();
    } catch (Throwable t) {
      System.err.println("Error running Client " + t);
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
    if (!cliParser.hasOption("jar") || !cliParser.hasOption("tachyon_home")
        || !cliParser.hasOption("master_address")) {
      printUsage();
      return false;
    }

    mHDFSAppMasterJar = cliParser.getOptionValue("jar");
    mTachyonHome = cliParser.getOptionValue("tachyon_home");
    mMasterAddress = cliParser.getOptionValue("master_address");

    mAppName = cliParser.getOptionValue("appname", "Tachyon");
    mAmPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    mAmQueue = cliParser.getOptionValue("queue", "default");
    mAmMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "256"));
    mAmVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
    mNumWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));

    Preconditions.checkArgument(mAmMemory > 0, "Invalid memory specified for application master, "
        + "exiting. Specified memory=" + mAmMemory);
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
    // Get a response of this application, containing information about the cluster
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    // Check the cluster has enough resource to launch the application master
    checkClusterResource(appResponse);

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

    Preconditions.checkArgument(mAmMemory <= maxMem,
        "ApplicationMaster memory specified above max threshold of cluster, specified=" + mAmMemory
            + ", max=" + maxMem);
    Preconditions.checkArgument(mAmVCores <= maxVCores,
        "ApplicationMaster virtual cores specified above max threshold of cluster, specified="
            + mAmVCores + ", max=" + maxVCores);
  }

  private void setupContainerLaunchContext() throws IOException {
    final String amCommand =
        new CommandBuilder(Environment.JAVA_HOME.$$() + "/bin/java").addArg("-Xmx256M")
            .addArg(mAppMasterMainClass).addArg(mNumWorkers).addArg(mTachyonHome)
            .addArg(mMasterAddress)
            .addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
            .addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr").toString();

    System.out.println("ApplicationMaster command: " + amCommand);
    mAmContainer.setCommands(Collections.singletonList(amCommand));

    // Setup jar for ApplicationMaster
    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    setupAppMasterJar(appMasterJar);
    mAmContainer.setLocalResources(Collections.singletonMap("tachyon.jar", appMasterJar));

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    mAmContainer.setEnvironment(appMasterEnv);
  }

  private void setupAppMasterJar(LocalResource appMasterJar) throws IOException {
    Path jarPath = new Path(mHDFSAppMasterJar); // known path to jar file on HDFS
    FileStatus jarStat = FileSystem.get(mYarnConf).getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    for (String c : mYarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
          c.trim(), ApplicationConstants.CLASS_PATH_SEPARATOR);
    }
    Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*", ApplicationConstants.CLASS_PATH_SEPARATOR);
  }

  /**
   * Sets up the application submission context.
   */
  private void setupApplicationSubmissionContext() {
    // set the application name
    mAppContext.setApplicationName(mAppName);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and vcores requirements
    Resource capability = Resource.newInstance(mAmMemory, mAmVCores);
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
