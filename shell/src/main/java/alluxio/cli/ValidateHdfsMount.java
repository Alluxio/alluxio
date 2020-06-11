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
import alluxio.cli.validation.ValidationTask;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A tool to validate an HDFS mount, before the path is mounted to Alluxio.
 * */
public class ValidateHdfsMount {
  private static final Logger LOG = LoggerFactory.getLogger(ValidateHdfsMount.class);

  private static final String USAGE =
          "USAGE: runHdfsMountTests [--readonly] [--shared] [--option <key=val>] <hdfsURI>"
                  + "runHdfsMountTests runs a set of validations against the given hdfs path.";

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
  private static final Option HELP_OPTION =
          Option.builder()
                  .longOpt("help")
                  .required(false)
                  .hasArg(false)
                  .desc("show help")
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
                  .addOption(HELP_OPTION).addOption(OPTION_OPTION);

  /**
   * Runs the subset of tests based on the target UFS type.
   * A validation task will run only if it matches the target type, or it matches all types.
   *
   * @param ufsPath the UFS path
   * @param ufsConf the UFS configurations
   * @return a list of task results for each task
   * */
  public static List<ValidationUtils.TaskResult> validateUfs(String ufsPath,
                                                             AlluxioConfiguration ufsConf)
          throws InterruptedException {
    Map<String, String> validateOpts = ImmutableMap.of();
    ValidateEnv tasks = new ValidateEnv(ufsPath, ufsConf);

    List<ValidationUtils.TaskResult> results = new ArrayList<>();
    for (Map.Entry<ValidationTask, String> entry : tasks.getTasks().entrySet()) {
      ValidationTask task = entry.getKey();
      Class clazz = task.getClass();
      if (clazz.isAnnotationPresent(ApplicableUfsType.class)) {
        ApplicableUfsType type = (ApplicableUfsType) clazz.getAnnotation(ApplicableUfsType.class);
        if (type.value() == ApplicableUfsType.Type.HDFS
                || type.value() == ApplicableUfsType.Type.ALL) {
          results.add(task.validate(validateOpts));
        }
      }
    }
    return results;
  }

  /**
   * Invokes {@link UnderFileSystemContractTest} to validate UFS operations.
   *
   * @param path the UFS path
   * @param conf the UFS conf
   * @return a {@link alluxio.cli.ValidationUtils.TaskResult} containing the validation result
   *        of the UFS operations
   * */
  public static ValidationUtils.TaskResult runUfsTests(String path, InstancedConfiguration conf) {
    try {
      UnderFileSystemContractTest test = new UnderFileSystemContractTest(path, conf);
      return test.runValidationTask();
    } catch (IOException e) {
      return new ValidationUtils.TaskResult(ValidationUtils.State.FAILED, "ufsTests",
              ValidationUtils.getErrorInfo(e), "");
    }
  }

  /**
   *
   * */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter help = new HelpFormatter();
    help.printHelp(USAGE, OPTIONS);
  }

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
      printHelp(String.format("Failed to parse arguments %s%n", Arrays.toString(args)));
      System.exit(1);
    }
    if (cmd.hasOption(HELP_OPTION.getLongOpt())) {
      printHelp("Showing usage for the command");
      System.exit(0);
    }

    args = cmd.getArgs();
    if (args.length < 1) {
      printHelp("Need at least 1 argument for <hdfsURI>!");
      System.exit(1);
    }

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
    List<ValidationUtils.TaskResult> results = validateUfs(ufsPath, ufsConf);

    // Run runUfsTests
    if (ufsConf.isReadOnly()) {
      LOG.debug("Ufs operations are skipped because the path is readonly.");
      results.add(new ValidationUtils.TaskResult(ValidationUtils.State.SKIPPED,
              UnderFileSystemContractTest.TASK_NAME,
              String.format("UFS path %s is readonly, skipped UFS operation tests.", ufsPath),
              ""));
    } else {
      results.add(runUfsTests(ufsPath, new InstancedConfiguration(ufsConf)));
    }

    // group by state
    Map<ValidationUtils.State, List<ValidationUtils.TaskResult>> map = new HashMap<>();
    results.stream().forEach((r) -> {
      map.computeIfAbsent(r.getState(), (k) -> new ArrayList<>()).add(r);
    });
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(map));

    System.exit(0);
  }
}
