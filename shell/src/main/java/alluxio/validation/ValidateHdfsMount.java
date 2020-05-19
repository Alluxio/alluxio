package alluxio.validation;

import alluxio.cli.UnderFileSystemContractTest;
import alluxio.cli.validation.HdfsValidationTask;
import alluxio.cli.validation.SecureHdfsValidationTask;
import alluxio.cli.validation.UfsSuperUserValidationTask;
import alluxio.cli.validation.ValidationTask;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.underfs.UnderFileSystemConfiguration;
import org.apache.commons.cli.*;

import java.util.*;

public class ValidateHdfsMount {
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
            new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION).addOption(OPTION_OPTION);

    public static List<ValidationTask> getValidationTasks(AlluxioConfiguration conf) {
        List<ValidationTask> tasks = new ArrayList<>();
        tasks.add(new HdfsValidationTask(conf));
        // TODO(jiacheng): get rid of these
        tasks.add(new SecureHdfsValidationTask("master", conf));
        tasks.add(new SecureHdfsValidationTask("worker", conf));
        tasks.add(new UfsSuperUserValidationTask(conf));
        return tasks;
    }

    public static void validateEnvChecks(AlluxioConfiguration conf) throws InterruptedException {
        // TODO(jiacheng): HdfsValidationTask reads from the option map
        Map<String, String> optionMap = new HashMap<>();

        List<ValidationTask> tasks = getValidationTasks(conf);
        for (ValidationTask t : tasks) {
            t.validate(optionMap);
        }
    }

    public static void runUfsTests() throws Exception {
        UnderFileSystemContractTest test = new UnderFileSystemContractTest();
        test.run();
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
        } catch (ParseException e) {
            System.exit(1);
        }
        args = cmd.getArgs();

        // Merge options from the command line option
        UnderFileSystemConfiguration conf = UnderFileSystemConfiguration.defaults(InstancedConfiguration.defaults());
        if (cmd.hasOption(READONLY_OPTION.getLongOpt())) {
            conf.setReadOnly(true);
        }
        if (cmd.hasOption(SHARED_OPTION.getLongOpt())) {
            conf.setShared(true);
        }
        if (cmd.hasOption(OPTION_OPTION.getLongOpt())) {
            Properties properties = cmd.getOptionProperties(OPTION_OPTION.getLongOpt());
            conf.merge(properties, Source.MOUNT_OPTION);
            System.out.format("Options from cmdline: %s%n", properties);
        }

        // Run validateEnv
        validateEnvChecks(conf);

        // Run runUfsTests
        // TODO(jiacheng): pass conf?
        runUfsTests();

        // TODO(jiacheng): how to build a mapping between check and result?
    }
}
