package alluxio.cli;

import alluxio.cli.validation.ApplicableUfsType;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.shell.CommandReturn;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A tool to validate an HDFS mount, before the paths is mounted to Alluxio.
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
  private static final Option LOCAL_OPTION =
          Option.builder().required(false).longOpt("local").hasArg(false)
                  .desc("running only on localhost").build();
  private static final Options OPTIONS =
          new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION)
                  .addOption(OPTION_OPTION).addOption(LOCAL_OPTION);

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

  /**
   * The entrance.
   *
   * @param args command line arguments
   * */
  public static void main(String[] args) throws Exception {
    // TODO(jiacheng): use jccommand?
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
    if (cmd.hasOption(LOCAL_OPTION.getLongOpt())) {
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

      // Serialize the results back to the calling node
      printResults(results);

      System.exit(0);
    }

    // Cluster mode
    LOG.info("Invoking the command remotely on the Alluxio cluster.");

    // how many nodes in the cluster
    Set<String> hosts = ConfigurationUtils.getServerHostnames(conf);
    ExecutorService executor = Executors.newFixedThreadPool(hosts.size());

    // Invoke validateHdfsMount locally on each host
    Map<String, CompletableFuture<CommandReturn>> resultFuture = new HashMap<>();
    for (String host : hosts) {
      LOG.info("validate hdfs mount on host {}", host);

      // We make the assumption that the Alluxio WORK_DIR is the same
      String workDir = conf.get(PropertyKey.WORK_DIR);
      String alluxioBinPath = Paths.get(workDir, "bin/alluxio")
              .toAbsolutePath().toString();
      LOG.info("host: {}, alluxio path {}", host, alluxioBinPath);

      String[] validateHdfsArgs =
              (String[]) ArrayUtils.addAll(
                      new String[]{alluxioBinPath, "runClass",
                              ValidateHdfsMount.class.getCanonicalName(), "--local"}, args);

      CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
        try {
          return ShellUtils.sshExecCommandWithOutput(host, validateHdfsArgs);
        } catch (Exception e) {
          LOG.error("Execution failed: ", e);
          return new CommandReturn(1, validateHdfsArgs, e.toString());
        }
      }, executor);
      resultFuture.put(host, future);
      LOG.info("Invoked local validateHdfs command on host {}", host);
    }

    // collect results
    Map<String, Map<ValidateUtils.State, List<ValidateUtils.TaskResult>>> hostToGroupedResults = new HashMap<>();
    for (Map.Entry<String, CompletableFuture<CommandReturn>> entry : resultFuture.entrySet()) {
      String host = entry.getKey();
      CommandReturn cr = entry.getValue().get();
      System.out.format("Host %s%nStatus: %s%n", host, cr.getExitCode());
      System.out.println(cr.getFormattedOutput());
      // Deserialize from JSON
      List<ValidateUtils.TaskResult> taskResults = parseTaskResults(cr.getOutput());
      Map<ValidateUtils.State, List<ValidateUtils.TaskResult>> groupedMap = new HashMap<>();
      // group by state
      taskResults.forEach((r) -> {
        groupedMap.computeIfAbsent(r.getState(), (k) -> new ArrayList<>()).add(r);
      });
      hostToGroupedResults.put(host, groupedMap);
    }

    // Parse and join results
    formatAndPrintGroupedResults(hostToGroupedResults);

    System.exit(0);
  }

  private static void printResults(List<ValidateUtils.TaskResult> results) throws Exception {
    String json = JsonSerializable.listToJson(results);
    System.out.println(json);
  }

  private static List<ValidateUtils.TaskResult> parseTaskResults(String json) throws JsonProcessingException {
    return new ObjectMapper().readValue(json, new TypeReference<List<ValidateUtils.TaskResult>>() {});
  }

  private static void formatAndPrintGroupedResults(Map<String, Map<ValidateUtils.State, List<ValidateUtils.TaskResult>>> resultMap) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(resultMap));
  }
}
