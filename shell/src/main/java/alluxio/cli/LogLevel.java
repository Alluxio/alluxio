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

import alluxio.Constants;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.util.network.HttpUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.LogInfo;
import alluxio.wire.WorkerNetAddress;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Sets or gets the log level for the specified server.
 */
@NotThreadSafe
public final class LogLevel {
  private static final String LOG_LEVEL = "logLevel";
  private static final String ROLE_WORKERS = "workers";
  private static final String ROLE_MASTER = "master";
  private static final String ROLE_WORKER = "worker";
  private static final String TARGET_SEPARATOR = ",";
  private static final String TARGET_OPTION_NAME = "target";
  private static final Option TARGET_OPTION =
      Option.builder()
          .required(false)
          .longOpt(TARGET_OPTION_NAME)
          .hasArg(true)
          .desc("<master|workers|host:webPort>."
              + " A list of targets separated by " + TARGET_SEPARATOR + " can be specified."
              + " host:webPort pair must be one of workers."
              + " Default target is master and all workers")
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
   * @exception ParseException if there is an error in parsing
   */
  public static void logLevel(String[] args) throws ParseException, IOException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);

    List<TargetInfo> targets = parseOptTarget(cmd);
    String logName = parseOptLogName(cmd);
    String level = parseOptLevel(cmd);

    for (TargetInfo targetInfo : targets) {
      setLogLevel(targetInfo, logName, level);
    }
  }

  private static List<TargetInfo> parseOptTarget(CommandLine cmd) throws IOException {
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
      targets = new String[]{ROLE_MASTER, ROLE_WORKERS};
    }
    return getTargetInfos(targets);
  }

  private static List<TargetInfo> getTargetInfos(String[] targets) throws IOException {
    List<TargetInfo> targetInfoList = new ArrayList<>();
    for (String target : targets) {
      if (target.equals(ROLE_MASTER)) {
        String masterHost = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_WEB);
        int masterPort = NetworkAddressUtils.getPort(ServiceType.MASTER_WEB);
        targetInfoList.add(new TargetInfo(masterHost, masterPort, ROLE_MASTER));
      } else if (target.equals(ROLE_WORKERS)) {
        AlluxioBlockStore alluxioBlockStore = AlluxioBlockStore.create();
        List<BlockWorkerInfo> workerInfoList = alluxioBlockStore.getAllWorkers();
        for (BlockWorkerInfo workerInfo : workerInfoList) {
          WorkerNetAddress netAddress = workerInfo.getNetAddress();
          targetInfoList.add(
              new TargetInfo(netAddress.getHost(), netAddress.getWebPort(), ROLE_WORKER));
        }
      } else if (target.contains(":")) {
        String[] hostPortPair = target.split(":");
        int port = Integer.parseInt(hostPortPair[1]);
        targetInfoList.add(new TargetInfo(hostPortPair[0], port, ROLE_WORKER));
      } else {
        throw new IOException("Unrecognized target argument: " + target);
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

  private static void setLogLevel(final TargetInfo targetInfo, String logName, String level)
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
    HttpUtils.post(uriBuilder.toString(), 5000, new HttpUtils.IProcessInputStream() {
      @Override
      public void process(InputStream inputStream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        LogInfo logInfo = mapper.readValue(inputStream, LogInfo.class);
        System.out.println(targetInfo.toString() + logInfo.toString());
      }
    });
  }

  /**
   * Sets or gets log level of master and worker through their REST API.
   *
   * @param args same arguments as {@link LogLevel}
   */
  public static void main(String[] args) {
    int exitCode = 1;
    try {
      logLevel(args);
      exitCode = 0;
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println(String.format("Failed to set log level: %s", e.getMessage()));
    }
    System.exit(exitCode);
  }

  private LogLevel() {} // this class is not intended for instantiation

  private static final class TargetInfo {
    private String mRole;
    private String mHost;
    private int mPort;

    public TargetInfo(String host, int port, String role) {
      mHost = host;
      mPort = port;
      mRole = role;
    }

    public int getPort() {
      return mPort;
    }

    public String getHost() {
      return mHost;
    }

    public String getRole() {
      return mRole;
    }

    @Override
    public String toString() {
      return mHost + ":" + mPort + "[" + mRole + "]";
    }
  }
}
