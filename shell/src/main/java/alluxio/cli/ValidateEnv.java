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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Arrays;

/**
 * Utility for checking Alluxio environment.
 */
public final class ValidateEnv {
  private static final String USAGE = "validateEnv COMMAND [NAME] [OPTIONS]\n\n"
      + "Validate environment for Alluxio.\n\n"
      + "COMMAND can be one of the following values:\n"
      + "local:   run all validation tasks on local\n"
      + "master:  run master validation tasks on local\n"
      + "worker:  run worker validation tasks on local\n"
      + "all:     run corresponding validation tasks on all master nodes and worker nodes\n"
      + "masters: run master validation tasks on all master nodes\n"
      + "workers: run worker validation tasks on all worker nodes\n\n"
      + "list:    list all validation tasks\n\n"
      + "For all commands except list:\n"
      + "NAME can be any task full name or prefix.\n"
      + "When NAME is given, only tasks with name starts with the prefix will run.\n"
      + "For example, specifying NAME \"master\" or \"ma\" will run both tasks named "
      + "\"master.rpc.port.available\" and \"master.web.port.available\" but not "
      + "\"worker.rpc.port.available\".\n"
      + "If NAME is not given, all tasks for the given TARGET will run.\n\n"
      + "OPTIONS can be a list of command line options. Each option has the"
      + " format \"-<optionName> [optionValue]\"\n";

  private static int runTasks(String target, String name, CommandLine cmd)
      throws InterruptedException {
    // Validate against root path
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    String rootPath = conf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    EnvValidationTool validate = new EnvValidationTool(rootPath, conf);

    boolean success;
    switch (target) {
      case "local":
      case "worker":
      case "master":
        success = validate.validateLocal(target, name, cmd);
        break;
      case "all":
        success = validate.validateMasters(name, cmd);
        success = validate.validateWorkers(name, cmd) && success;
        success = validate.validateLocal("cluster", name, cmd) && success;
        break;
      case "workers":
        success = validate.validateWorkers(name, cmd);
        break;
      case "masters":
        success = validate.validateMasters(name, cmd);
        break;
      default:
        printHelp("Invalid target.", EnvValidationTool.getOptions());
        return -2;
    }
    return success ? 0 : -1;
  }

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   * @param options options
   */
  public static void printHelp(String message, Options options) {
    System.err.println(message);
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(USAGE, options, true);
  }

  /**
   * Validates environment.
   *
   * @param argv list of arguments
   * @return 0 on success, -1 on validation failures, -2 on invalid arguments
   */
  public static int validate(String... argv) throws InterruptedException {
    Options options = EnvValidationTool.getOptions();
    if (argv.length < 1) {
      printHelp("Target not specified.", options);
      return -2;
    }
    String command = argv[0];
    String name = null;
    String[] args;
    int argsLength = 0;
    // Find all non-option command line arguments.
    while (argsLength < argv.length && !argv[argsLength].startsWith("-")) {
      argsLength++;
    }
    if (argsLength > 1) {
      name = argv[1];
      args = Arrays.copyOfRange(argv, 2, argv.length);
    } else {
      args = Arrays.copyOfRange(argv, 1, argv.length);
    }

    CommandLine cmd;
    try {
      cmd = parseArgsAndOptions(options, args);
    } catch (InvalidArgumentException e) {
      System.err.format("Invalid argument: %s.%n", e.getMessage());
      return -1;
    }
    if (command.equals("list")) {
      // Validate against root path
      AlluxioConfiguration conf = InstancedConfiguration.defaults();
      String rootPath = conf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
      EnvValidationTool task = new EnvValidationTool(rootPath, conf);
      task.printTasks();
      return 0;
    }
    return runTasks(command, name, cmd);
  }

  /**
   * Parses the command line arguments and options in {@code args}.
   *
   * After successful execution of this method, command line arguments can be
   * retrieved by invoking {@link CommandLine#getArgs()}, and options can be
   * retrieved by calling {@link CommandLine#getOptions()}.
   *
   * @param args command line arguments to parse
   * @return {@link CommandLine} object representing the parsing result
   * @throws InvalidArgumentException if command line contains invalid argument(s)
   */
  private static CommandLine parseArgsAndOptions(Options options, String... args)
      throws InvalidArgumentException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new InvalidArgumentException(
              "Failed to parse args for validateEnv", e);
    }
    return cmd;
  }

  /**
   * Validates Alluxio environment.
   *
   * @param args the arguments to specify which validation tasks to run
   */
  public static void main(String[] args) throws InterruptedException {
    System.exit(validate(args));
  }
}
