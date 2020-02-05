package alluxio.cli.bundler;

import alluxio.AlluxioTestDirectory;
import alluxio.cli.CommandUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

// TODO(jiacheng): integration test?
public class InfoCollectorAllTest {
  private static InstancedConfiguration mConf = new InstancedConfiguration(ConfigurationUtils.defaults());

  @BeforeClass
  public static final void beforeClass() throws Exception {
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    mConf.set(PropertyKey.CONF_DIR, targetDir);
    String confFile = mConf.get(PropertyKey.CONF_DIR);
    writeTempFile(confFile + "/masters");
  }

  // TODO(jiacheng): get hosts

  // TODO(jiacheng): load commands?

  // TODO(jiacheng): all sub commands are invoked
//  @Test
//  public void invokeCommandsOnAllHosts() {
//    when(CollaboratorWithStaticMethods.firstMethod(Mockito.anyString()))
//            .thenReturn("Hello Baeldung!");
//    when(CollaboratorWithStaticMethods.secondMethod()).thenReturn("Nothing special");
//  }

  public static void writeTempFile(String path) throws IOException
  {
    String fileContent = "localhost";

    BufferedWriter writer = new BufferedWriter(new FileWriter(path));
    writer.write(fileContent);
    writer.close();
  }

  @Test
  public void exec() throws IOException, TimeoutException {
    String confDir = mConf.get(PropertyKey.CONF_DIR);
    System.out.println("Confg dir "+confDir);

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fail earlier for CLIs
    mConf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    mConf.set(PropertyKey.WORK_DIR, "/Users/jiachengliu/Documents/Alluxio/alluxio-jiacheng/alluxio");
    InfoCollectorAll shellAll = new InfoCollectorAll(mConf);

    // For each host execute
    // TODO(jiacheng): get hosts from static util call
    List<String> allHosts = new ArrayList<>();
    allHosts.add("localhost");

    // TODO(jiacheng): can this be done in a better way?
    System.out.println(String.format("Init thread pool for %s hosts", allHosts.size()));
    ExecutorService ee = Executors.newFixedThreadPool(allHosts.size());

    // Invoke infoBundle on each host
    List<CompletableFuture<CommandReturn>> futureList = new ArrayList<>();
    for (String host : allHosts) {
      System.out.println(String.format("Execute InfoCollector on host %s", host));

      CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
        String workDir = mConf.get(PropertyKey.WORK_DIR);
        String alluxioBinPath = Paths.get(workDir, "bin/alluxio")
                .toAbsolutePath().toString();
        System.out.println(String.format("host: %s, alluxio path %s", host, alluxioBinPath));

        // TODO(jiacheng): Now we assume alluxio is on the same path on every machine
        try {
          String[] infoBundleArgs = new String[]{
                  alluxioBinPath, "fsadmin", "report"
          };
          CommandReturn cr = ShellUtils.sshExecCommandWithOutput(host, infoBundleArgs);
          return cr;
        } catch (Exception e) {
          System.out.println("Exec failed");
          return new CommandReturn(1, e.toString());
        }
      }, ee);
      futureList.add(future);
      System.out.println(String.format("Invoked infoBundle command on host %s", host));
    }

    // Collect the execution results
    List<CommandReturn> results;
    try {
      results = all(futureList).get(30, TimeUnit.MILLISECONDS);
      System.out.println(String.format("%s results completed", results.size()));
      for (CommandReturn cr : results) {
        System.out.println(cr.getFormattedOutput());
      }
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Command failed!");
      throw new IOException(e);
    }
  }

  public static <T> CompletableFuture<List<T>> all(List<CompletableFuture<T>> futures) {
    CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);

    return CompletableFuture.allOf(cfs)
            .thenApply(ignored -> futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList())
            );
  }

  @Test
  public void runCommand() throws IOException, AlluxioException {
    String confDir = mConf.get(PropertyKey.CONF_DIR);
    System.out.println("Confg dir "+confDir);

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    String[] mockArgs = new String[]{"all", targetDir.getAbsolutePath()};

    InfoCollectorAll.main(mockArgs);

    // The final result will be in a tarball
    File[] files = targetDir.listFiles();
    for (File f : files) {
      System.out.println(String.format("File %s", f.getName()));
      if (!f.isDirectory() && !f.getName().endsWith(".tar.gz")) {
        System.out.println(String.format("%s", new String(Files.readAllBytes(f.toPath()))));
      } else if (f.isDirectory()) {
        for (File ff : f.listFiles()) {
          System.out.println(String.format("Nested file %s size %s", ff.getName(), ff.getTotalSpace()));
//          System.out.println(String.format("%s", new String(Files.readAllBytes(ff.toPath()))));
        }
      }
    }
  }
}
