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

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.HttpUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.LogInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.job.JobMasterClientContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Sets or gets the log level for the specified server.
 */
@NotThreadSafe
@PublicApi
public final class LogLevel {
  private static final Logger LOG = LoggerFactory.getLogger(LogLevel.class);

  public static final String LOG_LEVEL = "logLevel";
  public static final String ROLE_WORKERS = "workers";
  public static final String ROLE_MASTER = "master";
  public static final String ROLE_MASTERS = "masters";
  public static final String ROLE_WORKER = "worker";
  public static final String ROLE_JOB_MASTER = "job_master";
  public static final String ROLE_JOB_MASTERS = "job_masters";
  public static final String ROLE_JOB_WORKER = "job_worker";
  public static final String ROLE_JOB_WORKERS = "job_workers";
  public static final String TARGET_SEPARATOR = ",";
  public static final String TARGET_OPTION_NAME = "target";
  private static final Option TARGET_OPTION =
      Option.builder()
          .required(false)
          .longOpt(TARGET_OPTION_NAME)
          .hasArg(true)
          .desc("<master|workers|job_master|job_workers|host:webPort>."
              + " A list of targets separated by " + TARGET_SEPARATOR + " can be specified."
              + " host:webPort pair must be one of workers."
              + " Default target is master, job master, all workers and all job workers.")
          .build();
  private static final String LOG_NAME_OPTION_NAME = "logName";
  private static final Option LOG_NAME_OPTION =
      Option.builder()
          .required(true)
          .longOpt(LOG_NAME_OPTION_NAME)
          .hasArg(true)
          .desc("The logger's name(e.g. alluxio.master.file.DefaultFileSystemMaster)"
              + " you want to get or set level.")
          .build();
  private static final String LEVEL_OPTION_NAME = "level";
  private static final Option LEVEL_OPTION =
      Option.builder()
          .required(false)
          .longOpt(LEVEL_OPTION_NAME)
          .hasArg(true)
          .desc("The log level to be set.").build();
  private static final Options OPTIONS = new Options()
      .addOption(TARGET_OPTION)
      .addOption(LOG_NAME_OPTION)
      .addOption(LEVEL_OPTION);

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter help = new HelpFormatter();
    help.printHelp(LOG_LEVEL, OPTIONS, true);
  }

  /**
   * Implements log level setting and getting.
   *
   * @param args list of arguments contains target, logName and level
   * @param alluxioConf configurations
   * @exception ParseException if there is an error in parsing
   */
  public static void logLevel(String[] args, AlluxioConfiguration alluxioConf)
      throws ParseException, IOException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);

    List<TargetInfo> targets = parseOptTarget(cmd, alluxioConf);
    String logName = parseOptLogName(cmd);
    String level = parseOptLevel(cmd);

    for (TargetInfo targetInfo : targets) {
      setLogLevel(targetInfo, logName, level, alluxioConf);
    }
  }

  /**
   * Parse the command options to a list of targets endpoints.
   *
   * @param cmd the command
   * @param conf Alluxio configuration
   * @return a list of {@link TargetInfo}
   * */
  public static List<TargetInfo> parseOptTarget(CommandLine cmd, AlluxioConfiguration conf)
      throws IOException {
    String[] targets;
    if (cmd.hasOption(TARGET_OPTION_NAME)) {
      String argTarget = cmd.getOptionValue(TARGET_OPTION_NAME);
      if (StringUtils.isBlank(argTarget)) {
        throw new IOException("Option " + TARGET_OPTION_NAME + " can not be blank.");
      } else if (argTarget.contains(TARGET_SEPARATOR)) {
        targets = argTarget.split(TARGET_SEPARATOR);
      } else {
        targets = new String[]{argTarget};
      }
    } else {
      // By default we set on all targets (master/workers/job_master/job_workers)
      targets = new String[]{ROLE_MASTER, ROLE_JOB_MASTER, ROLE_WORKERS, ROLE_JOB_WORKERS};
    }
    return getTargetInfos(targets, conf);
  }

  private static List<TargetInfo> getTargetInfos(String[] targets, AlluxioConfiguration conf)
      throws IOException {
    // Trim the elements
    Set<String> targetSet = Arrays.stream(targets).map(String::trim).collect(Collectors.toSet());
    List<TargetInfo> targetInfoList = new ArrayList<>();

    // Allow plural form for the master/job_master and print a notice
    if (targetSet.contains(ROLE_MASTERS)) {
      System.out.println("The logLevel command will only take effect on the primary master, "
              + "instead of on all the masters. ");
      targetSet.remove(ROLE_MASTERS);
      targetSet.add(ROLE_MASTER);
      System.out.println("Target `masters` is replaced with `master`.");
    }
    if (targetSet.contains(ROLE_JOB_MASTERS)) {
      System.out.println("The logLevel command will only take effect on the primary job master, "
              + "instead of on all the masters. ");
      targetSet.remove(ROLE_JOB_MASTERS);
      targetSet.add(ROLE_JOB_MASTER);
      System.out.println("Target `job_masters` is replaced with `job_master`.");
    }

    ClientContext clientContext = ClientContext.create(conf);
    // Created only when needed by master and workers
    FileSystemContext fsContext = null;
    // Created only when needed by the job master and job workers
    JobMasterClient jobClient = null;

    // Process each target
    for (String target : targetSet) {
      if (target.isEmpty()) {
        continue;
      } else if (target.equals(ROLE_MASTER)) {
        if (fsContext == null) {
          fsContext = FileSystemContext.create(clientContext);
        }
        String masterHost = fsContext.getMasterAddress().getHostName();
        int masterPort = NetworkAddressUtils.getPort(ServiceType.MASTER_WEB, conf);
        TargetInfo master = new TargetInfo(masterHost, masterPort, ROLE_MASTER);
        targetInfoList.add(master);
      } else if (target.equals(ROLE_JOB_MASTER)) {
        if (jobClient == null) {
          jobClient = JobMasterClient.Factory.create(JobMasterClientContext
                  .newBuilder(clientContext).build());
        }
        String jobMasterHost = jobClient.getAddress().getHostName();
        int jobMasterPort = NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_WEB, conf);
        TargetInfo jobMaster = new TargetInfo(jobMasterHost, jobMasterPort, ROLE_JOB_MASTER);
        targetInfoList.add(jobMaster);
      } else if (target.equals(ROLE_WORKERS)) {
        if (fsContext == null) {
          fsContext = FileSystemContext.create(ClientContext.create(conf));
        }
        List<BlockWorkerInfo> workerInfoList = fsContext.getCachedWorkers();
        if (workerInfoList.size() == 0) {
          System.out.println("No workers found");
          System.exit(1);
        }
        for (BlockWorkerInfo workerInfo : workerInfoList) {
          WorkerNetAddress netAddress = workerInfo.getNetAddress();
          TargetInfo worker = new TargetInfo(netAddress.getHost(),
                  netAddress.getWebPort(), ROLE_WORKER);
          targetInfoList.add(worker);
        }
      } else if (target.equals(ROLE_JOB_WORKERS)) {
        if (jobClient == null) {
          jobClient = JobMasterClient.Factory.create(JobMasterClientContext
                  .newBuilder(clientContext).build());
        }
        List<JobWorkerHealth> jobWorkerInfoList = jobClient.getAllWorkerHealth();
        if (jobWorkerInfoList.size() == 0) {
          System.out.println("No job workers found");
          System.exit(1);
        }
        int jobWorkerPort = conf.getInt(PropertyKey.JOB_WORKER_WEB_PORT);
        for (JobWorkerHealth jobWorkerInfo : jobWorkerInfoList) {
          String jobWorkerHost = jobWorkerInfo.getHostname();
          TargetInfo jobWorker = new TargetInfo(jobWorkerHost, jobWorkerPort, ROLE_JOB_WORKER);
          targetInfoList.add(jobWorker);
        }
      } else if (target.contains(":")) {
        String[] hostPortPair = target.split(":");
        int port = Integer.parseInt(hostPortPair[1]);
        String role = inferRoleFromPort(port, conf);
        LOG.debug("Port {} maps to role {}", port, role);
        TargetInfo unspecifiedTarget = new TargetInfo(hostPortPair[0], port, role);
        System.out.format("Role inferred from port: %s%n", unspecifiedTarget);
        targetInfoList.add(unspecifiedTarget);
      } else {
        throw new IOException(String.format("Unrecognized target argument: %s. "
                + "Please pass the targets in the form of <host>:<port>, "
                + "with comma as the separator.", target));
      }
    }
    return targetInfoList;
  }

  private static String parseOptLogName(CommandLine cmd) {
    String argName = cmd.getOptionValue(LOG_NAME_OPTION_NAME);
    if (StringUtils.isNotBlank(argName)) {
      return argName;
    }
    return "";
  }

  private static String parseOptLevel(CommandLine cmd) {
    if (cmd.hasOption(LEVEL_OPTION_NAME)) {
      String argLevel = cmd.getOptionValue(LEVEL_OPTION_NAME);
      if (StringUtils.isNotBlank(argLevel)) {
        return argLevel;
      }
    }
    return null;
  }

  private static void setLogLevel(final TargetInfo targetInfo, String logName, String level,
                                  AlluxioConfiguration alluxioConf)
      throws IOException {
    URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme("http");
    uriBuilder.setHost(targetInfo.getHost());
    uriBuilder.setPort(targetInfo.getPort());
    uriBuilder.setPath(Constants.REST_API_PREFIX + "/" + targetInfo.getRole() + "/" + LOG_LEVEL);
    uriBuilder.addParameter(LOG_NAME_OPTION_NAME, logName);
    if (level != null) {
      uriBuilder.addParameter(LEVEL_OPTION_NAME, level);
    }
    LOG.info("Setting log level on {}", uriBuilder.toString());
    HttpUtils.post(uriBuilder.toString(), "", 5000, inputStream -> {
      ObjectMapper mapper = new ObjectMapper();
      LogInfo logInfo = mapper.readValue(inputStream, LogInfo.class);
      System.out.println(targetInfo.toString() + logInfo.toString());
    });
  }

  private static String inferRoleFromPort(int port, AlluxioConfiguration conf) {
    if (port == NetworkAddressUtils.getPort(ServiceType.MASTER_WEB, conf)) {
      return ROLE_MASTER;
    } else if (port == NetworkAddressUtils.getPort(ServiceType.WORKER_WEB, conf)) {
      return ROLE_WORKER;
    } else if (port == NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_WEB, conf)) {
      return ROLE_JOB_MASTER;
    } else if (port == NetworkAddressUtils.getPort(ServiceType.JOB_WORKER_WEB, conf)) {
      return ROLE_JOB_WORKER;
    } else {
      throw new IllegalArgumentException(String.format(
              "Unrecognized port in %s. Please make sure the port is in %s",
              port,
              Arrays.toString(new PropertyKey[]{
                PropertyKey.MASTER_WEB_PORT, PropertyKey.WORKER_WEB_PORT,
                PropertyKey.JOB_MASTER_WEB_PORT, PropertyKey.JOB_WORKER_WEB_PORT
              })));
    }
  }

  /**
   * Sets or gets log level of master and worker through their REST API.
   *
   * @param args same arguments as {@link LogLevel}
   */
  public static void main(String[] args) {
    int exitCode = 1;
    try {
      logLevel(args, new InstancedConfiguration(ConfigurationUtils.defaults()));
      exitCode = 0;
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
    } catch (IOException e) {
      System.err.println("Failed to set log level:");
      e.printStackTrace();
    }
    System.exit(exitCode);
  }

  private LogLevel() {} // this class is not intended for instantiation

  /**
   * Object that represents a REST endpoint that logLevel sends HTTP request to.
   * */
  public static final class TargetInfo {
    private String mRole;
    private String mHost;
    private int mPort;

    /**
     * Constructor.
     *
     * @param host hostname
     * @param port port
     * @param role the Alluxio component name(part of URL)
     * */
    public TargetInfo(String host, int port, String role) {
      mHost = host;
      mPort = port;
      mRole = role;
    }

    /**
     * @return port
     * */
    public int getPort() {
      return mPort;
    }

    /**
     * @return hostname
     * */
    public String getHost() {
      return mHost;
    }

    /**
     * @return component name
     * */
    public String getRole() {
      return mRole;
    }

    @Override
    public String toString() {
      return mHost + ":" + mPort + "[" + mRole + "]";
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof TargetInfo)) {
        return false;
      }
      TargetInfo otherTarget = (TargetInfo) other;
      return mRole.equals(otherTarget.mRole) && mHost.equals(otherTarget.mHost)
              && mPort == otherTarget.mPort;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mRole, mHost, mPort);
    }
  }
}
