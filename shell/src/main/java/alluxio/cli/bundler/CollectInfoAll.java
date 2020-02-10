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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.shell.CommandReturn;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import alluxio.util.io.FileUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
public class CollectInfoAll extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(CollectInfoAll.class);
  private static final String FINAL_TARBALL_NAME =  "alluxio-cluster-info-%s.tar.gz";

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.of();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.of();

  // TODO(jiacheng): what is a good max thread num?
  private ExecutorService mExecutor;

  /**
   * Creates a new instance of {@link CollectInfoAll}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public CollectInfoAll(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  // TODO(jiacheng): phase 2 specify hosts from cmdline
  /**
   * Finds all hosts in the Alluxio cluster.
   * We assume the masters and workers cover all the nodes in the cluster.
   *
   * @return a set of hostnames in the cluster
   * */
  public Set<String> getHosts() {
    String confDirPath = mConfiguration.get(PropertyKey.CONF_DIR);
    System.out.format("Looking for masters and workers in %s%n", confDirPath);
    Set<String> hosts = new HashSet<>();
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "masters"));
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "workers"));
    System.out.format("Found %s hosts%n", hosts.size());
    return hosts;
  }

  /**
   * Main method, starts a new CollectInfoAll shell.
   * CollectInfoAll will SSH to all hosts and invoke {@link CollectInfo}.
   * Then collect the tarballs generated on each of the hosts to the localhost.
   * And tarball all results into one final tarball.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret = 0;

    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fail earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    CollectInfoAll shellAll = new CollectInfoAll(conf);

    // Validate commands
    if (argv.length < 2) {
      shellAll.printUsage();
      System.exit(-1);
    }
    String targetDir = argv[1];

    List<String> allHosts = new ArrayList<>(shellAll.getHosts());
    System.out.format("Init thread pool for %s hosts%n", allHosts.size());
    shellAll.mExecutor = Executors.newFixedThreadPool(allHosts.size());

    // Invoke infoBundle on each host
    List<CompletableFuture<CommandReturn>> sshFutureList = new ArrayList<>();
    for (String host : allHosts) {
      System.out.format("Execute CollectInfo on host %s%n", host);

      CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
        // We make the assumption that the Alluxio WORK_DIR is the same
        String workDir = conf.get(PropertyKey.WORK_DIR);
        String alluxioBinPath = Paths.get(workDir, "bin/alluxio")
                .toAbsolutePath().toString();
        System.out.format("host: %s, alluxio path %s%n", host, alluxioBinPath);

        try {
          String[] infoBundleArgs =
                  (String[]) ArrayUtils.addAll(new String[]{alluxioBinPath, "infoBundle"}, argv);
          CommandReturn cr = ShellUtils.sshExecCommandWithOutput(host, infoBundleArgs);
          return cr;
        } catch (Exception e) {
          LOG.error("Execution failed %s", e);
          return new CommandReturn(1, e.toString());
        }
      }, shellAll.mExecutor);
      sshFutureList.add(future);
      System.out.format("Invoked infoBundle command on host %s%n", host);
    }

    // Collect SSH execution results
    List<String> sshSucceededHosts =
            shellAll.collectCommandReturnsFromHosts(sshFutureList, allHosts);

    // If all executions failed, clean up and exit
    if (sshSucceededHosts.size() == 0) {
      System.err.println("Failed to invoke infoBundle command on all hosts!");
      shellAll.close();
      System.exit(1);
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
        LOG.debug("Copying %s:%s to %s", host, fromPath, toPath);

        try {
          CommandReturn cr =
                  ShellUtils.scpCommandWithOutput(host, fromPath, toPath, false);
          return cr;
        } catch (IOException e) {
          LOG.error("Execution failed %s", e);
          return new CommandReturn(1, e.toString());
        }
      }, shellAll.mExecutor);
      scpFutures.add(future);
    }

    List<String> scpSucceededHosts =
            shellAll.collectCommandReturnsFromHosts(scpFutures, sshSucceededHosts);
    System.out.format("Tarballs of %d/%d hosts copied to %s%n",
            scpSucceededHosts.size(), allHosts.size(), tempDir.getAbsolutePath());

    // If all executions failed, clean up and exit
    if (scpSucceededHosts.size() == 0) {
      System.err.println("Failed to collect tarballs from all hosts!");
      shellAll.close();
      System.exit(1);
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

    // Destroy the thread pool and resources
    shellAll.close();
    System.exit(ret);
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
      System.out.format("Command successful on %d/%d hosts.%n",
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
    return "collectInfoAll";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    // We do not load any commands here.
    // All calls will be delegated to CollectInfo local calls on each host.
    return Collections.EMPTY_MAP;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (!mExecutor.isShutdown()) {
      mExecutor.shutdownNow();
    }
  }
}
