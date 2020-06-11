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

import alluxio.cli.validation.ApplicableUfsType;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A tool to validate an HDFS mount, before the path is mounted to Alluxio.
 * */
public class ValidateHdfsMount {
  private static final Logger LOG = LoggerFactory.getLogger(ValidateHdfsMount.class);

  private static final Option READONLY_OPTION =
          Option.builder()
                  .longOpt("readonly")
                  .required(false)
                  .hasArg(false)
                  .desc("mount point is readonly in Alluxio")
                  .build();
  private static final Option SHARED_OPTION =
          Option.builder()
                  .longOpt("shared")
                  .required(false)
                  .hasArg(false)
                  .desc("mount point is shared")
                  .build();
  private static final Option OPTION_OPTION =
          Option.builder()
                  .longOpt("option")
                  .required(false)
                  .hasArg(true)
                  .numberOfArgs(2)
                  .argName("key=value")
                  .valueSeparator('=')
                  .desc("options associated with this mount point")
                  .build();
  private static final Options OPTIONS =
          new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION)
                  .addOption(OPTION_OPTION);

  /**
   * Invokes {@link UnderFileSystemContractTest} to validate UFS operations.
   *
   * @param path the UFS path
   * @param conf the UFS conf
   * @return a {@link alluxio.cli.ValidateUtils.TaskResult} containing the validation result
   *        of the UFS operations
   * */
  public static ValidateUtils.TaskResult runUfsTests(String path, InstancedConfiguration conf) {
    try {
      UnderFileSystemContractTest test = new UnderFileSystemContractTest(path, conf);
      return test.runValidationTask();
    } catch (IOException e) {
      return new ValidateUtils.TaskResult(ValidateUtils.State.FAILED, "ufsTests",
              ValidateUtils.getErrorInfo(e), "");
    }
  }

  // TODO(jiacheng): print help

  /**
   * The entrance.
   *
   * @param args command line arguments
   * */
  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      System.exit(1);
    }
    args = cmd.getArgs();
    String ufsPath = args[0];

    InstancedConfiguration conf = InstancedConfiguration.defaults();
    // Merge options from the command line option
    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(conf);
    if (cmd.hasOption(READONLY_OPTION.getLongOpt())) {
      ufsConf.setReadOnly(true);
    }
    if (cmd.hasOption(SHARED_OPTION.getLongOpt())) {
      ufsConf.setShared(true);
    }
    if (cmd.hasOption(OPTION_OPTION.getLongOpt())) {
      Properties properties = cmd.getOptionProperties(OPTION_OPTION.getLongOpt());
      ufsConf.merge(properties, Source.MOUNT_OPTION);
      LOG.debug("Options from cmdline: {}", properties);
    }

    // Run validateEnv
    Map<String, String> validateOpts = ImmutableMap.of();
    ValidateEnv validate = new ValidateEnv(ufsPath, ufsConf);
    List<ValidateUtils.TaskResult> results = validate.validateUfs(
            ApplicableUfsType.Type.HDFS, validateOpts);

    // Run runUfsTests
    if (ufsConf.isReadOnly()) {
      LOG.debug("Ufs operations are skipped because the path is readonly.");
      results.add(new ValidateUtils.TaskResult(ValidateUtils.State.SKIPPED,
              UnderFileSystemContractTest.TASK_NAME,
              String.format("UFS path %s is readonly, skipped UFS operation tests.", ufsPath),
              ""));
    } else {
      results.add(runUfsTests(ufsPath, new InstancedConfiguration(ufsConf)));
    }

    // group by state
    Map<ValidateUtils.State, List<ValidateUtils.TaskResult>> map = new HashMap<>();
    results.stream().forEach((r) -> {
      map.computeIfAbsent(r.getState(), (k) -> new ArrayList<>()).add(r);
    });
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(map));

    System.exit(0);
  }
}
