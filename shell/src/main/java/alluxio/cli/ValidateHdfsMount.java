package alluxio.cli;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;
import alluxio.underfs.UnderFileSystemConfiguration;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
   * Print help with the message.
   *
   * @param message the message
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

    // group by state
    HdfsValidationTool tool = new HdfsValidationTool(ufsPath, ufsConf);
    Map<ValidationUtils.State, List<ValidationUtils.TaskResult>> map = tool.runValidations();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(map));

    System.exit(0);
  }
}
