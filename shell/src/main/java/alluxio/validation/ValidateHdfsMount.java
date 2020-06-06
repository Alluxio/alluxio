package alluxio.validation;

import alluxio.cli.Command;
import alluxio.cli.UnderFileSystemContractTest;
import alluxio.cli.ValidateEnv;
import alluxio.cli.ValidateUtils;
import alluxio.cli.validation.*;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.shell.CommandReturn;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.mapred.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Native;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
            new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION).addOption(OPTION_OPTION)
            .addOption(LOCAL_OPTION);

    // TODO(jiacheng)
    public static ValidateUtils.TaskResult runUfsTests(String path, InstancedConfiguration conf) {
        try {
            UnderFileSystemContractTest test = new UnderFileSystemContractTest(path, conf);
            return test.runValidationTask();
        } catch (IOException e) {
            return new ValidateUtils.TaskResult(ValidateUtils.State.FAILED, "ufsTests", ValidateUtils.getErrorInfo(e), "");
        }
    }

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
        System.out.format("ufs path is %s%n", ufsPath);

        InstancedConfiguration conf = InstancedConfiguration.defaults();

        if (cmd.hasOption(LOCAL_OPTION.getLongOpt())) {
            // Merge options from the command line option
            // TODO(jiacheng): this should work for root and nested mount
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
                LOG.info("Options from cmdline: {}", properties);
            }

            // Run validateEnv
            Map<String, String> validateOpts = ImmutableMap.of();
            ValidateEnv validate = new ValidateEnv(ufsPath, ufsConf);
            List<ValidateUtils.TaskResult> results = validate.validateUfs(ApplicableUfsType.Type.HDFS, validateOpts);

            // Run runUfsTests
            if (ufsConf.isReadOnly()) {
                results.add(new ValidateUtils.TaskResult(ValidateUtils.State.SKIPPED,
                        UnderFileSystemContractTest.TASK_NAME,
                        String.format("UFS path %s is readonly, skipped UFS operation tests.", ufsPath), ""));
            } else {
                results.add(runUfsTests(ufsPath, new InstancedConfiguration(ufsConf)));
            }

            // TODO(jiacheng): how to build a mapping between check and result?
            // Convert to output and print
            System.out.println(results);

            printResults(results);

            System.exit(0);
        }

        // Cluster mode

        // how many nodes in the cluster
        Set<String> hosts = ConfigurationUtils.getServerHostnames(conf);
        ExecutorService executor = Executors.newFixedThreadPool(hosts.size());

        // Invoke collectInfo locally on each host
        List<CompletableFuture<CommandReturn>> sshFutureList = new ArrayList<>();
        for (String host : hosts) {
            LOG.info("validate hdfs mount on host {}", host);

            // We make the assumption that the Alluxio WORK_DIR is the same
            String workDir = conf.get(PropertyKey.WORK_DIR);
            String alluxioBinPath = Paths.get(workDir, "bin/alluxio")
                    .toAbsolutePath().toString();
            System.out.format("host: %s, alluxio path %s%n", host, alluxioBinPath);

            String[] validateHdfsArgs =
                    (String[]) ArrayUtils.addAll(
                            new String[]{alluxioBinPath, "runClass", ValidateHdfsMount.class.getCanonicalName(), "--local"}, args);

            CompletableFuture<CommandReturn> future = CompletableFuture.supplyAsync(() -> {
                try {
                    CommandReturn cr = ShellUtils.sshExecCommandWithOutput(host, validateHdfsArgs);
                    return cr;
                } catch (Exception e) {
                    LOG.error("Execution failed %s", e);
                    return new CommandReturn(1, validateHdfsArgs, e.toString());
                }
            }, executor);
            sshFutureList.add(future);
            LOG.info("Invoked local validateHdfs command on host {}", host);
        }

        // collect results
        CompletableFuture[] cfs = sshFutureList.toArray(new CompletableFuture[0]);

        List<CommandReturn> results = CompletableFuture.allOf(cfs)
                .thenApply(f -> sshFutureList.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                ).get();
        for (CommandReturn cr : results) {
            System.out.println(cr.getFormattedOutput());
        }

        // Parse and join results

        System.exit(0);
    }

    public static void printResults(List<ValidateUtils.TaskResult> results) {
        Map<ValidateUtils.State, List<ValidateUtils.TaskResult>> map = new HashMap<>();

        // group by state
        results.stream().forEach((r) -> {
            map.computeIfAbsent(r.getState(), (k) -> new ArrayList<>()).add(r);
        });
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(map));
    }
}
