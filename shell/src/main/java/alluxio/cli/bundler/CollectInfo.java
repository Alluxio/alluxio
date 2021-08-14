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

package alluxio.cli.bundler;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.bundler.command.AbstractCollectInfoCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import alluxio.util.io.FileUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Class for collecting various information about all nodes in the cluster.
 */
public class CollectInfo extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(CollectInfo.class);
  private static final String USAGE =
      "collectInfo [--max-threads <threadNum>] [--local] [--help] "
          + "[--exclude-logs <filename-prefixes>] [--include-logs <filename-prefixes>] "
          + "[--additional-logs <filename-prefixes>] [--start-time <datetime>] "
          + "[--end-time <datetime>] COMMAND <outputPath>\n\n"
          + "collectInfo runs a set of sub-commands which collect information "
          + "about your Alluxio cluster.\nIn the end of the run, "
          + "the collected information will be written to files and bundled into one tarball.\n"
          + "COMMAND can be one of the following values:\n"
          + "all:                runs all the commands below.\n"
          + "collectAlluxioInfo: runs a set of Alluxio commands to collect information about "
          + "the Alluxio cluster.\n"
          + "collectConfig:      collects the configuration files under ${ALLUXIO_HOME}/config/.\n"
          + "collectEnv:         runs a set of linux commands to collect information about "
          + "the cluster.\n"
          + "collectJvmInfo:     collects jstack from the JVMs.\n"
          + "collectLog:         collects the log files under ${ALLUXIO_HOME}/logs/.\n"
          + "collectMetrics:     collects Alluxio system metrics.\n\n"
          + "<outputPath>        the directory you want the collected tarball to be in\n\n"
          + "WARNING: This command MAY bundle credentials. To understand the risks refer "
          + "to the docs here.\nhttps://docs.alluxio.io/os/user/edge/en/operation/"
          + "Troubleshooting.html#collect-alluxio-cluster-information\n";
  private static final String FINAL_TARBALL_NAME =  "alluxio-cluster-info-%s.tar.gz";

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.of();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.of();

  private static final String TARBALL_NAME = "alluxio-info.tar.gz";
  private ExecutorService mExecutor;

  private static final String MAX_THREAD_OPTION_NAME = "max-threads";
  private static final Option THREAD_NUM_OPTION =
          Option.builder().required(false).longOpt(MAX_THREAD_OPTION_NAME).hasArg(true)
                  .desc("the number of threads this command uses\n"
                          + "By default it allocates one thread for each host.\n"
                          + "Use a smaller number to constrain the network IO when "
                          + "transmitting tarballs.")
                  .build();
  private static final String LOCAL_OPTION_NAME = "local";
  private static final Option LOCAL_OPTION =
          Option.builder().required(false).longOpt(LOCAL_OPTION_NAME).hasArg(false)
                  .desc("specifies this command should only collect information "
                          + "about the localhost")
                  .build();
  private static final String HELP_OPTION_NAME = "help";
  private static final Option HELP_OPTION =
      Option.builder().required(false).longOpt(HELP_OPTION_NAME).hasArg(false)
          .desc("shows the help message").build();
  // Build the options for collectInfo, then aggregate local options for each sub-command
  private static final Options OPTIONS = loadOptions(new Options()
      .addOption(THREAD_NUM_OPTION)
      .addOption(LOCAL_OPTION)
      .addOption(HELP_OPTION));

  // Load the options defined in each sub-command class
  private static Options loadOptions(Options options) {
    Reflections reflections = new Reflections(Command.class.getPackage().getName());
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      try {
        for (Field f : cls.getDeclaredFields()) {
          if (f.getName().equals("OPTIONS")) {
            Options clsOptions = ((Options) f.get(null));
            if (clsOptions == null) {
              continue;
            }
            for (Option o : clsOptions.getOptions()) {
              options.addOption(o);
            }
          }
        }
      } catch (IllegalAccessException e) {
        LOG.warn("Failed to load OPTIONS from class {}: {}",
                cls.getCanonicalName(), e.getMessage());
      }
    }
    return options;
  }

  /**
   * Creates a new instance of {@link CollectInfo}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public CollectInfo(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  /**
   * Finds all hosts in the Alluxio cluster.
   * We assume the masters and workers cover all the nodes in the cluster.
   * This command now relies on conf/masters and conf/workers to contain
   * the nodes in the cluster.
   * This is the same requirement as bin/alluxio-start.sh.
   * TODO(jiacheng): phase 2 specify hosts from cmdline
   * TODO(jiacheng): phase 2 cross-check with the master for which nodes are in the cluster
   *
   * @return a set of hostnames in the cluster
   * */
  public Set<String> getHosts() {
    String confDirPath = mConfiguration.get(PropertyKey.CONF_DIR);
    System.out.format("Looking for masters and workers in %s%n", confDirPath);
    Set<String> hosts = ConfigurationUtils.getServerHostnames(mConfiguration);
    System.out.format("Found %s hosts%n", hosts.size());
    return hosts;
  }

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter help = new HelpFormatter();
    help.setWidth(200);
    help.printHelp(USAGE, OPTIONS);
  }

  /**
   * Main method, starts a new CollectInfo shell.
   * CollectInfo will SSH to all hosts and invoke {@link CollectInfo} with --local option.
   * Then collect the tarballs generated on each of the hosts to the localhost.
   * And tarball all results into one final tarball.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    // Parse cmdline args
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, argv, true /* stopAtNonOption */);
    } catch (ParseException e) {
      return;
    }
    String[] args = cmd.getArgs();

    // Print help message
    if (cmd.hasOption(HELP_OPTION_NAME)) {
      printHelp("");
      System.exit(0);
    }

    // Create the shell instance
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Reduce the RPC retry max duration to fail earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    CollectInfo shell = new CollectInfo(conf);

    // Validate command args
    if (args.length < 2) {
      printHelp(String.format("Command requires at least %s arguments (%s provided)%n",
              2, argv.length));
      System.exit(-1);
    }

    // Choose mode based on option
    int ret;
    if (cmd.hasOption(LOCAL_OPTION_NAME)) {
      System.out.println("Executing collectInfo locally");
      ret = shell.collectInfoLocal(cmd);
    } else {
      System.out.println("Executing collectInfo on all nodes in the cluster");
      ret = shell.collectInfoRemote(cmd);
    }

    // Clean up before exiting
    shell.close();
    System.exit(ret);
  }

  // Convert the command line back to a list of arguments
  private List<String> cmdLineToArgs(CommandLine cmd) {
    List<String> args = new ArrayList<>();
    for (Option opt : cmd.getOptions()) {
      if (opt.equals(LOCAL_OPTION) || opt.equals(THREAD_NUM_OPTION)) {
        continue;
      }
      args.add("--" + opt.getLongOpt());
      if (opt.hasArg()) {
        args.add(opt.getValue());
      }
    }
    args.addAll(cmd.getArgList());
    return args;
  }

  /**
   * Finds all nodes in the cluster.
   * Then invokes collectInfo with --local option on each of them locally.
   * Collects the generated tarball from each node.
   * And generates a final tarball as the result.
   *
   * @param cmdLine the parsed CommandLine
   * @return exit code
   * */
  private int collectInfoRemote(CommandLine cmdLine) throws IOException {
    int ret = 0;
    String[] args = cmdLine.getArgs();
    String targetDir = args[1];

    // Execute the command on all hosts
    List<String> allHosts = new ArrayList<>(getHosts());
    System.out.format("Init thread pool for %s hosts%n", allHosts.size());
    int threadNum = allHosts.size();
    if (cmdLine.hasOption("threads")) {
      int maxThreadNum = Integer.parseInt(cmdLine.getOptionValue(MAX_THREAD_OPTION_NAME));
      LOG.info("Max thread number is {}", maxThreadNum);
      threadNum = Math.min(maxThreadNum, threadNum);
    }
    LOG.info("Use {} threads", threadNum);
    mExecutor = Executors.newFixedThreadPool(threadNum);

    // Invoke collectInfo locally on each host
    List<CompletableFuture<CommandReturn>> sshFutureList = new ArrayList<>();
    for (String host : allHosts) {
      System.out.format("Execute collectInfo on host %s%n", host);

      CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
        // We make the assumption that the Alluxio WORK_DIR is the same
        String workDir = mConfiguration.get(PropertyKey.WORK_DIR);
        String alluxioBinPath = Paths.get(workDir, "bin/alluxio")
                .toAbsolutePath().toString();
        System.out.format("host: %s, alluxio path %s%n", host, alluxioBinPath);

        String[] collectInfoArgs =
                (String[]) ArrayUtils.addAll(
                        new String[]{alluxioBinPath, "collectInfo", "--local"},
                        cmdLineToArgs(cmdLine).toArray(new String[0]));
        System.out.format("Invoking command %s%n", Arrays.toString(collectInfoArgs));
        try {
          CommandReturn cr = ShellUtils.sshExecCommandWithOutput(host, collectInfoArgs);
          return cr;
        } catch (Exception e) {
          LOG.error("Execution failed %s", e);
          return new CommandReturn(1, collectInfoArgs, e.toString());
        }
      }, mExecutor);
      sshFutureList.add(future);
      System.out.format("Invoked local collectInfo command on host %s%n", host);
    }

    // Collect SSH execution results
    List<String> sshSucceededHosts =
            collectCommandReturnsFromHosts(sshFutureList, allHosts);

    // If all executions failed, skip the next step
    if (sshSucceededHosts.size() == 0) {
      System.err.println("Failed to invoke local collectInfo command on all hosts!");
      return 1;
    }

    // Collect tarballs from where the SSH command completed
    File tempDir = Files.createTempDir();
    List<File> filesFromHosts = new ArrayList<>();
    List<CompletableFuture<CommandReturn>> scpFutures =
            new ArrayList<>(allHosts.size());
    for (String host : sshSucceededHosts) {
      // Create dir for the host
      File tarballFromHost = new File(tempDir, host);
      tarballFromHost.mkdir();
      filesFromHosts.add(tarballFromHost);

      // Async execute the SCP step
      CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
        System.out.format("Collecting tarball from host %s%n", host);
        String fromPath = Paths.get(targetDir, CollectInfo.TARBALL_NAME)
                .toAbsolutePath().toString();
        String toPath = tarballFromHost.getAbsolutePath();
        LOG.debug("Copying {}:{} to {}", host, fromPath, toPath);

        try {
          CommandReturn cr =
                  ShellUtils.scpCommandWithOutput(host, fromPath, toPath, false);
          return cr;
        } catch (IOException e) {
          // An unexpected error occurred that caused this IOException
          LOG.error("Execution failed on {}", e);
          return new CommandReturn(1, e.toString());
        }
      }, mExecutor);
      scpFutures.add(future);
    }

    List<String> scpSucceededHosts =
            collectCommandReturnsFromHosts(scpFutures, sshSucceededHosts);
    System.out.format("Tarballs of %d/%d hosts copied%n",
            scpSucceededHosts.size(), allHosts.size());

    // If all executions failed, clean up and exit
    if (scpSucceededHosts.size() == 0) {
      System.err.println("Failed to collect tarballs from all hosts!");
      return 2;
    }

    // Generate a final tarball containing tarballs from each host
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    String finalTarballpath = Paths.get(targetDir,
            String.format(FINAL_TARBALL_NAME, dtf.format(LocalDateTime.now())))
            .toAbsolutePath().toString();
    TarUtils.compress(finalTarballpath, filesFromHosts.toArray(new File[0]));
    System.out.println("Final tarball compressed to " + finalTarballpath);

    // Delete the temp dir
    try {
      FileUtils.delete(tempDir.getPath());
    } catch (IOException e) {
      LOG.warn("Failed to delete temp dir {}", tempDir.toString());
    }

    return ret;
  }

  /**
   * Executes collectInfo command locally.
   * And generates a tarball with all the information collected.
   *
   * @param cmdLine the parsed CommandLine
   * @return exit code
   * */
  private int collectInfoLocal(CommandLine cmdLine) throws IOException {
    int ret = 0;
    String[] args = cmdLine.getArgs();

    // Determine the command and working dir path
    String subCommand = args[0];
    String targetDirPath = args[1];

    // There are 2 cases:
    // 1. Execute "all" commands
    // 2. Execute a single command
    List<File> filesToCollect = new ArrayList<>();
    if (subCommand.equals("all")) {
      // Case 1. Execute "all" commands
      System.out.println("Execute all child commands");
      String[] childArgs = Arrays.copyOf(args, args.length);
      for (Command cmd : getCommands()) {
        System.out.format("Executing %s%n", cmd.getCommandName());

        // Replace the action with the command to execute
        childArgs[0] = cmd.getCommandName();
        // Do the best effort to finish all other commands if one fails
        try {
          int childRet = executeAndAddFile(childArgs, cmdLine, filesToCollect);
          // If any of the commands failed, treat as failed
          if (ret == 0 && childRet != 0) {
            System.err.format("Command %s failed%n", cmd.getCommandName());
            ret = childRet;
          }
        } catch (AlluxioException e) {
          System.err.format("Command %s failed with exception:%n", cmd.getCommandName());
          e.printStackTrace(System.err);
        }
      }
    } else {
      // Case 2. Execute a single command
      try {
        int childRet = executeAndAddFile(args, cmdLine, filesToCollect);
        if (childRet != 0) {
          ret = childRet;
        }
      } catch (AlluxioException e) {
        System.err.format("Command failed with exception:%n");
        e.printStackTrace(System.err);
        return 1;
      }
    }

    // TODO(jiacheng): phase 2 add an option to disable bundle
    // Generate bundle
    System.out.format("Files to collect: %s%n", filesToCollect);
    System.out.format("Archiving dir %s%n", targetDirPath);

    String tarballPath = Paths.get(targetDirPath, TARBALL_NAME).toAbsolutePath().toString();
    if (filesToCollect.size() == 0) {
      System.err.format("No files to add. Tarball %s will be empty!%n", tarballPath);
      return 2;
    }
    TarUtils.compress(tarballPath, filesToCollect.toArray(new File[0]));
    System.out.println("Archiving finished");

    return ret;
  }

  private int executeAndAddFile(String[] argv, CommandLine cmdLine, List<File> filesToCollect)
          throws IOException, AlluxioException {
    // The argv length has been validated
    String subCommand = argv[0];
    String targetDirPath = argv[1];
    System.out.format("subcommand %s targetDir %s%n", subCommand, targetDirPath);

    AbstractCollectInfoCommand cmd = this.findCommand(subCommand);

    if (cmd == null) {
      // Unknown command (we did not find the cmd in our dict)
      printHelp(String.format("%s is an unknown command.%n", subCommand));
      return 1;
    }
    int ret = cmd.run(cmdLine);

    // File to collect
    File infoCmdOutputFile = cmd.generateOutputFile(targetDirPath,
            cmd.getCommandName());
    filesToCollect.add(infoCmdOutputFile);

    return ret;
  }

  private AbstractCollectInfoCommand findCommand(String cmdName) {
    for (Command c : this.getCommands()) {
      if (c.getCommandName().equals(cmdName)) {
        return (AbstractCollectInfoCommand) c;
      }
    }
    return null;
  }

  /**
   * Collects the results of ALL futures from the hosts.
   * Returns the list of hosts where the execution was successful,
   * for the next step.
   * */
  private List<String> collectCommandReturnsFromHosts(
          List<CompletableFuture<CommandReturn>> futureList, List<String> hosts) {
    // Collect the execution results
    List<CommandReturn> results;
    try {
      results = collectAllFutures(futureList).get();
      System.out.format("Results collected from %d hosts%n", results.size());
    } catch (InterruptedException | ExecutionException e) {
      System.err.format("Failed to collect the results. Error is %s%n", e.getMessage());
      LOG.error("Error: %s", e);
      return Collections.EMPTY_LIST;
    }

    // Record the list of hosts where the results are successfully collected
    if (results.size() != hosts.size()) {
      System.out.format("Error occurred while collecting information on %d/%d hosts%n",
              hosts.size() - results.size());
      // TODO(jiacheng): phase 2 find out what error occurred
      return Collections.EMPTY_LIST;
    } else {
      List<String> successfulHosts = new ArrayList<>();
      for (int i = 0; i < hosts.size(); i++) {
        CommandReturn c = results.get(i);
        String host = hosts.get(i);
        if (c.getExitCode() != 0) {
          System.out.format("Execution failed on host %s%n", host);
          System.out.println(c.getFormattedOutput());
          continue;
        }
        successfulHosts.add(host);
      }
      System.out.format("Command executed successfully on %d/%d hosts.",
              successfulHosts.size(), hosts.size());
      return successfulHosts;
    }
  }
  /**
   * Waits for ALL futures to complete and returns a list of results.
   * If any future completes exceptionally then the resulting future
   * will also complete exceptionally.
   *
   * @param <T> this is the type the {@link CompletableFuture} contains
   * @param futures a list of futures to collect
   * @return a {@link CompletableFuture} of all futures
   */
  public static <T> CompletableFuture<List<T>> collectAllFutures(
          List<CompletableFuture<T>> futures) {
    CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);

    return CompletableFuture.allOf(cfs)
            .thenApply(f -> futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList())
            );
  }

  @Override
  protected String getShellName() {
    return "collectInfo";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    // Give each command the configuration
    return CommandUtils.loadCommands(
            CollectInfo.class.getPackage().getName(),
            new Class[] {FileSystemContext.class},
            new Object[] {FileSystemContext.create(mConfiguration)});
  }

  @Override
  public void close() throws IOException {
    super.close();
    // Shutdown thread pool if not empty
    if (mExecutor != null && !mExecutor.isShutdown()) {
      mExecutor.shutdownNow();
    }
  }
}
