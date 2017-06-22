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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Set and get the specify target specify log's level.
 */
@NotThreadSafe
public final class LogLevel {

  private static final String LOG_LEVEL = "logLevel";
  private static final String TARGET_OPTION_NAME = "target";
  private static final Option TATGET_OPTION =
      Option.builder().required(false).longOpt(TARGET_OPTION_NAME).hasArg(true)
          .desc("<master|workers|host:port>."
              + " Multi target split by ',' host:port pair must be one of workers."
              + " Default target is master and all workers").build();
  private static final String LOG_NAME_OPTION_NAME = "logName";
  private static final Option LOG_NAME_OPTION =
      Option.builder().required(true).longOpt(LOG_NAME_OPTION_NAME).hasArg(true)
          .desc("The log name you want to get or set level.").build();
  private static final String LEVEL_OPTION_NAME = "level";
  private static final Option LEVEL_OPTION =
      Option.builder().required(false).longOpt(LEVEL_OPTION_NAME).hasArg(true)
          .desc("The level of the log what you specified.").build();
  private static final Options OPTIONS = new Options()
      .addOption(TATGET_OPTION)
      .addOption(LOG_NAME_OPTION)
      .addOption(LEVEL_OPTION);

  private static final String USAGE =
      "logLevel --logName=LOGNAME [--target=<master|workers|host:port>] [--level=LEVEL]";
  public static final String TARGET_SEPARATOR = ",";

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
   * @return 0 on success, 1 on failures
   */
  public static int logLevel(String... args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
      return 1;
    }
    List<TargetInfo> targetInfoList = new ArrayList<>();

    String[] targets = parseOptTarget(cmd);
    String logName = parseOptLogName(cmd);
    String level = parseOptLevel(cmd);

    for (String target : targets) {
      if (target.contains("master")) {
        String masterHost = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_WEB);
        int masterPort = NetworkAddressUtils.getPort(ServiceType.MASTER_WEB);
        targetInfoList.add(new TargetInfo(masterHost, masterPort, "master"));
      } else if (target.contains("workers")) {
        AlluxioBlockStore alluxioBlockStore = AlluxioBlockStore.create();
        try {
          List<BlockWorkerInfo> workerInfoList = alluxioBlockStore.getWorkerInfoList();
          for (BlockWorkerInfo workerInfo : workerInfoList) {
            WorkerNetAddress netAddress = workerInfo.getNetAddress();
            targetInfoList.add(
                new TargetInfo(netAddress.getHost(), netAddress.getWebPort(), "worker"));
          }
        } catch (IOException e) {
          e.printStackTrace();
          return 1;
        }
      } else if (target.contains(":")) {
        String[] hostPortPair = target.split(":");
        int port = Integer.parseInt(hostPortPair[1]);
        targetInfoList.add(new TargetInfo(hostPortPair[0], port, "worker"));
      }
    }

    for (TargetInfo targetInfo : targetInfoList) {
      if (handleTarget(logName, level, targetInfo)) {
        return 1;
      }
    }
    return 0;
  }

  private static String[] parseOptTarget(CommandLine cmd) {
    String[] targets;
    if (cmd.hasOption(TARGET_OPTION_NAME)) {
      String argTarget = cmd.getOptionValue(TARGET_OPTION_NAME);
      if (StringUtils.isBlank(argTarget)) {
        targets = new String[]{"master", "workers"};
      } else if (argTarget.contains(TARGET_SEPARATOR)) {
        targets = argTarget.split(TARGET_SEPARATOR);
      } else {
        targets = new String[]{argTarget};
      }
    } else {
      targets = new String[]{"master", "workers"};
    }
    return targets;
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

  private static boolean handleTarget(String logName, String level, TargetInfo targetInfo) {
    URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme("http");
    uriBuilder.setHost(targetInfo.getHost());
    uriBuilder.setPort(targetInfo.getPort());
    uriBuilder.setPath(Constants.REST_API_PREFIX + "/" + targetInfo.getRole() + "/" + LOG_LEVEL);
    uriBuilder.addParameter(LOG_NAME_OPTION_NAME, logName);
    if (level != null) {
      uriBuilder.addParameter(LEVEL_OPTION_NAME, level);
    }
    InputStream inputStream = null;
    try {
      inputStream = HttpUtils.post(uriBuilder.toString(), 5000);
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (inputStream != null) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        LogInfo logInfo = mapper.readValue(inputStream, LogInfo.class);
        System.out.println(targetInfo.toString() + logInfo.toString());
      } catch (IOException e) {
        e.printStackTrace();
        return true;
      }
    } else {
      return true;
    }
    return false;
  }

  /**
   * Set or get log level of Alluxio servers.
   *
   * @param args the arguments to specify the arguments of this command
   */
  public static void main(String[] args) {
    System.exit(logLevel(args));
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
