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
import com.google.common.base.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
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
          .desc("<master|workers|host:port>. Multi target split by ','.").build();
  private static final String LOGNAME_OPTION_NAME = "logName";
  private static final Option LOGNAME_OPTION =
      Option.builder().required(true).longOpt(LOGNAME_OPTION_NAME).hasArg(true)
          .desc("log name.").build();
  private static final String LEVEL_OPTION_NAME = "level";
  private static final Option LEVEL_OPTION =
      Option.builder().required(false).longOpt(LEVEL_OPTION_NAME).hasArg(true)
          .desc("<master|workers|host:port>.").build();
  private static final Options OPTIONS = new Options()
      .addOption(TATGET_OPTION)
      .addOption(LOGNAME_OPTION)
      .addOption(LEVEL_OPTION);

  private static final String USAGE =
      "logLevel --logName=LOGNAME [--target=<master|workers|host:port>] [--level=LEVEL]";

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter help = new HelpFormatter();
    help.printHelp(USAGE, OPTIONS);
  }

  /**
   * Implements log level setting and getting.
   *
   * @param args list of arguments
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
    Preconditions.checkNotNull(cmd, "Unable to parse input args");
    List<TargetInfo> addrList = new ArrayList<>();
    String logName = "";
    String level = null;
    String[] targets;

    String argName = cmd.getOptionValue(LOGNAME_OPTION_NAME);
    if (StringUtils.isNotBlank(argName)) {
      logName = argName;
    }

    if (cmd.hasOption(TARGET_OPTION_NAME)) {
      String argTarget = cmd.getOptionValue(TARGET_OPTION_NAME);
      if (StringUtils.isBlank(argTarget)) {
        targets = new String[]{"master", "workers"};
      } else if (argTarget.contains(",")) {
        targets = argTarget.split(",");
      } else {
        targets = new String[]{argTarget};
      }
    } else {
      targets = new String[]{"master", "workers"};
    }
    for (String target : targets) {
      if (target.contains("master")) {
        String masterHost = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_WEB);
        int masterPort = NetworkAddressUtils.getPort(ServiceType.MASTER_WEB);
        addrList.add(new TargetInfo(masterHost + ":" + masterPort, "master"));
      } else if (target.contains("workers")) {
        AlluxioBlockStore alluxioBlockStore = AlluxioBlockStore.create();
        try {
          List<BlockWorkerInfo> workerInfoList = alluxioBlockStore.getWorkerInfoList();
          for (BlockWorkerInfo workerInfo : workerInfoList) {
            WorkerNetAddress netAddress = workerInfo.getNetAddress();
            addrList.add(
                new TargetInfo(netAddress.getHost() + ":" + netAddress.getWebPort(), "worker"));
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else if (target.contains(":")) {
        addrList.add(new TargetInfo(target, "worker"));
      }
    }

    if (cmd.hasOption(LEVEL_OPTION_NAME)) {
      String argLevel = cmd.getOptionValue(LEVEL_OPTION_NAME);
      if (StringUtils.isNotBlank(argLevel)) {
        level = argLevel;
      }
    }

    for (TargetInfo targetInfo : addrList) {
      String url = "http://" + targetInfo.getAddres() + Constants.REST_API_PREFIX + "/"
          + targetInfo.getRole() + "/" + LOG_LEVEL + "?" + LOGNAME_OPTION_NAME + "=" + logName;
      if (level != null) {
        url += "&" + LEVEL_OPTION_NAME + "=" + level;
      }
      String result = HttpUtils.sendPost(url, 5000);
      ObjectMapper mapper = new ObjectMapper();
      try {
        LogInfo logInfo = mapper.readValue(result, LogInfo.class);
        System.out.println(
            targetInfo.getAddres() + "[" + targetInfo.getRole() + "]" + logInfo.toString());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return 0;
  }

  /**
   * Prints Alluxio configuration.
   *
   * @param args the arguments to specify the unit (optional) and configuration key (optional)
   */
  public static void main(String[] args) {
    System.exit(logLevel(args));
  }

  private LogLevel() {} // this class is not intended for instantiation

  static final class TargetInfo {
    private String mAddres;
    private String mRole;

    public TargetInfo(String address, String role) {
      mAddres = address;
      mRole = role;
    }

    public String getAddres() {
      return mAddres;
    }

    public String getRole() {
      return mRole;
    }
  }
}
