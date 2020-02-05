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

import alluxio.AlluxioTestDirectory;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.bundler.command.TarUtils;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.shell.CommandReturn;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
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
public class InfoCollectorAll extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(InfoCollectorAll.class);

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
          .build();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.<String>builder()
          .build();

  // TODO(jiacheng): inspect this executor
  private ExecutorService mExecutor;

  /**
   * Creates a new instance of {@link FileSystemShell}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public InfoCollectorAll(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  // TODO(jiacheng): consider specifying hosts from cmdline
  /**
   * Finds all hosts in the Alluxio cluster.
   * We assume the masters and workers cover all the nodes in the cluster.
   *
   * @return a set of hostnames in the cluster
   * */
  public Set<String> getHosts() {
    // File
    String confDirPath = mConfiguration.get(PropertyKey.CONF_DIR);
    System.out.println(String.format("Looking for masters and workers in %s", confDirPath));
    Set<String> hosts = new HashSet<>();
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "masters"));
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "workers"));
    System.out.println(String.format("Found %s hosts: %s", hosts.size(), hosts));

    return hosts;
  }

  /**
   * Main method, starts a new InfoCollectorAll shell.
   * InfoCollectorAll will SSH to all hosts and invoke {@link InfoCollector}.
   * Then collect the tarballs generated on each of the hosts to the localhost.
   * And tarball all results into one final tarball.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret = 0;

    String action = argv[0];
    String targetDir = argv[1];

    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fail earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    InfoCollectorAll shellAll = new InfoCollectorAll(conf);

    // Validate commands
    if (argv.length < 2) {
      shellAll.printUsage();
      System.exit(-1);
    }

    // For each host execute
    // TODO(jiacheng): get hosts from static util call
    List<String> allHosts = new ArrayList<>(shellAll.getHosts());

    // TODO(jiacheng): can this be done in a better way?
    System.out.println(String.format("Init thread pool for %s hosts", allHosts.size()));
    shellAll.mExecutor = Executors.newFixedThreadPool(allHosts.size());

    // Invoke infoBundle on each host
    List<CompletableFuture<CommandReturn>> futureList = new ArrayList<>();
    for (String host : allHosts) {
      System.out.println(String.format("Execute InfoCollector on host %s", host));

      CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
        String workDir = conf.get(PropertyKey.WORK_DIR);
        String alluxioBinPath = Paths.get(workDir, "bin/alluxio")
                .toAbsolutePath().toString();
        System.out.println(String.format("host: %s, alluxio path %s", host, alluxioBinPath));

        // TODO(jiacheng): Now we assume alluxio is on the same path on every machine
        try {
          String[] infoBundleArgs = new String[]{
            alluxioBinPath, "infoBundle", action, targetDir
          };
          CommandReturn cr = ShellUtils.sshExecCommandWithOutput(host, infoBundleArgs);
          return cr;
        } catch (Exception e) {
          // TODO(jiacheng): What exceptions can be thrown here?
          LOG.error("Execution failed %s", e);
          return new CommandReturn(1, e.toString());
        }
      }, shellAll.mExecutor);
      futureList.add(future);
      System.out.println(String.format("Invoked infoBundle command on host %s", host));
    }

    // Collect the execution results
    List<CommandReturn> results;
    try {
      results = all(futureList).get();
      System.out.println(String.format("%s results completed", results.size()));
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Command failed!");
      throw new IOException(e);
    }

    // TODO(jiacheng): better error handling?
    // Inspect all command results
    if (results.size() != allHosts.size()) {
      System.out.println("Host size mismatch!");
    } else {
      for (int i = 0; i < allHosts.size(); i++) {
        CommandReturn c = results.get(i);
        String host = allHosts.get(i);
        if (c.getExitCode() != 0) {
          System.out.println(String.format("Command failed on host %s", host));
          System.out.println(c.getFormattedOutput());
        } else {
          System.out.println(String.format("Success on host %s", host));
        }
      }
    }

    // Collect all tarballs to local
    // TODO(jiacheng): move to variable
    File tempDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");

    List<File> filesFromHosts = new ArrayList<>();
    for (String host : allHosts) {
      // Create dir for the host
      File tarballFromHost = new File(tempDir, host);
      tarballFromHost.mkdir();
      filesFromHosts.add(tarballFromHost);

      System.out.println(String.format("Collecting tarball from host %s", host));
      // TODO(jiacheng): move to variable
      String fromPath = Paths.get(targetDir, "collectAll.tar.gz")
                          .toAbsolutePath().toString();
      String toPath = tarballFromHost.getAbsolutePath();
      System.out.println(String.format("Copying %s:%s to %s", host, fromPath, toPath));

      // TODO(jiacheng): asynch this
      CommandReturn cr = ShellUtils.scpCommandWithOutput(host, fromPath, toPath, false);

      if (cr.getExitCode() != 0) {
        System.out.println(String.format("Failed on host ", host));
        System.out.println(cr.getFormattedOutput());
      }
    }

    System.out.println("All tarballs copied to " + tempDir.getAbsolutePath());
    System.out.println(String.format("Tarballs from hosts in directories: %s",
                        filesFromHosts));

    // Generate a final tarball containing tarballs from each host
    String finalTarballpath = Paths.get(targetDir, "InfoCollectorAll.tar.gz")
                                .toAbsolutePath().toString();
    TarUtils.compress(finalTarballpath, filesFromHosts.toArray(new File[0]));
    System.out.println("Final tarball compressed to " + finalTarballpath);

    System.exit(ret);
  }

  /**
   * Waits for ALL futures to complete and returns a list of results.
   * If *any* future completes exceptionally then the resulting future
   * will also complete exceptionally.
   * Ref: https://stackoverflow.com/a/36261808/4933827
   *
   * @param <T> this is the type the {@link CompletableFuture} contains
   * @param futures a list of futures to collect
   * @return a {@link CompletableFuture} of all futures
   */
  public static <T> CompletableFuture<List<T>> all(List<CompletableFuture<T>> futures) {
    CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);

    return CompletableFuture.allOf(cfs)
            .thenApply(ignored -> futures.stream()
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
    // Give each command the configuration
    Map<String, Command> commands = CommandUtils.loadCommands(
            InfoCollector.class.getPackage().getName(),
            new Class[] {FileSystemContext.class},
            new Object[] {FileSystemContext.create(mConfiguration)});
    System.out.println(String.format("Loaded commands %s", commands));
    return commands;
  }
}
