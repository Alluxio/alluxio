package alluxio.cli.bundler;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShell;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.shell.CommandReturn;
import alluxio.util.ConfigurationUtils;

import alluxio.util.ShellUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.Property;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.ThreadPool;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

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
  public Set<String> getHosts() {
    // File
    String confDirPath = mConfiguration.get(PropertyKey.CONF_DIR);
    Set<String> hosts = new HashSet<>();
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "masters"));
    hosts.addAll(CommandUtils.readNodeList(confDirPath, "workers"));
    System.out.println(String.format("Found %s hosts: %s", hosts.size(), hosts));

    return hosts;
  }

  /**
   * Main method, starts a new Shell.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret = 0;

    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fail earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    InfoCollectorAll shellAll = new InfoCollectorAll(conf);

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
          CommandReturn cr = ShellUtils.execCommandWithOutput(alluxioBinPath, "infoBundle");
          return cr;
        } catch (Exception e) {
          // TODO(jiacheng): What to do here?
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
      for (int i=0; i<allHosts.size(); i++) {
        CommandReturn c = results.get(i);
        String host = allHosts.get(i);
        if (c.getExitCode() != 0) {
          System.out.println(String.format("Command failed on host %s",host));
          System.out.println(c.getFormattedOutput());
        } else {
          System.out.println(String.format("Success on host %s", host));
        }
      }
    }

    // Collect all tarballs to local
    String logPath = conf.get(PropertyKey.LOGS_DIR);
    // TODO(jiacheng): move to variable
    String collectTarballPath = Paths.get(logPath, "infoBundleAll").toAbsolutePath().toString();
    System.out.println(String.format("Copying all remote tarballs to %s", collectTarballPath));
    for (String host : allHosts) {
      System.out.println(String.format("Collecting tarball from host %s", host));
      // TODO(jiacheng): move to variable
      String remoteTarballPath = Paths.get(logPath, "infoBundleLocal").toAbsolutePath().toString();
      String localTarballPath = Paths.get(collectTarballPath, host).toAbsolutePath().toString();
      System.out.println(String.format("Copying %s:%s to %s", host, remoteTarballPath, localTarballPath));

      // TODO(jiacheng): asynch this
      CommandReturn cr = ShellUtils.scpCommandWithOutput(host, remoteTarballPath, localTarballPath,false);

      if (cr.getExitCode() !=0) {
        System.out.println(String.format("Failed on host ", host));
        System.out.println(cr.getFormattedOutput());
      }
    }

    // TODO(jiacheng): Generate final tarball
    // TODO(jiacheng): tarball feature to util


    System.exit(ret);
  }

  // Waits for *all* futures to complete and returns a list of results.
// If *any* future completes exceptionally then the resulting future will also complete exceptionally.

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
  //TODO(jiacheng): load commands
  protected Map<String, Command> loadCommands() {
    return null;
  }
}
