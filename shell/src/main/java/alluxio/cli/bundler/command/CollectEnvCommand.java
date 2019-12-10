package alluxio.cli.bundler.command;

import alluxio.cli.CommandUtils;
import alluxio.cli.ConfigurationDocGenerator;
import alluxio.cli.bundler.RunCommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectEnvCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectEnvCommand.class);

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("ignores existing work")
                  .build();

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION);
  }

  private static class UnixCommand {
    String mName;
    String mCmd;
    String mBetterVersion;

    UnixCommand(String name, String cmd, String betterVersion) {
      mName = name;
      mCmd = cmd;
      mBetterVersion = betterVersion;
    }

    String[] getCommand() {
      String[] bashCmd = new String[]{"bash", "-c"};
      if (mBetterVersion != null) {
        return mBetterVersion.split(" ");
      } else {
        return mCmd.split(" ");
      }
    }
  }

  private List<UnixCommand> mCommands;

  public CollectEnvCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
    mCommands = new ArrayList<>();
    registerCommands();
  }

  private void registerCommands() {
    mCommands.add(new UnixCommand("ps", "ps -ef | grep alluxio*", null));
    mCommands.add(new UnixCommand("env", "env", null));
    mCommands.add(new UnixCommand("top", "top -b -n 1", "atop -b -n 1"));
    mCommands.add(new UnixCommand("mount", "mount", null));
    mCommands.add(new UnixCommand("df", "df -H", null));
    mCommands.add(new UnixCommand("ulimit", "ulimit -Ha", null));
    mCommands.add(new UnixCommand("uname", "uname -a", null));
    mCommands.add(new UnixCommand("hostname", "hostname", null));
    mCommands.add(new UnixCommand("host ip", "hostname -i", null));
    mCommands.add(new UnixCommand("host fqdn", "hostname -v", null));
    mCommands.add(new UnixCommand("list Alluxio home", String.format("ls -al -R %s",
            mFsContext.getClusterConf().get(PropertyKey.HOME)), null));
    mCommands.add(new UnixCommand("dig", "dig $(hostname -i)", null));
    mCommands.add(new UnixCommand("nslookup", "nslookup $(hostname -i)", null));
    // TODO(jiacheng): Does this stop?
    mCommands.add(new UnixCommand("dstat", "dstat -cdgilmnprsty", null));
  }

  @Override
  public String getCommandName() {
    return "collectEnv";
  }

  @Override
  public boolean hasSubCommand() {
    return false;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    String targetDir = getDestDir(cl);
    boolean force = cl.hasOption("f");

    // Skip if previous work can be reused.
    if (!force && foundPreviousWork(targetDir)) {
      LOG.info("Found previous work. Skipped.");
      return ret;
    }

    // Output buffer stream
    StringWriter outputBuffer = new StringWriter();

    for (UnixCommand cmd : mCommands) {
      String[] command = cmd.getCommand();
      outputBuffer.write(String.format("Running cmd %s", Arrays.toString(command)));
      RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(command);
      String cmdResult = String.format("StatusCode:%s\nStdOut:\n%s\nStdErr:\n%s", cr.getStatusCode(),
              cr.getStdOut(), cr.getStdErr());
      outputBuffer.write(cmdResult);
    }

    // output the buffer
    File outputFile = getOutputFile(targetDir, String.format("%s.txt", getCommandName()));
    LOG.info(String.format("Finished all commands. Writing to output file %s", outputFile.getAbsolutePath()));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());

    return ret;
  }

  @Override
  public String getUsage() {
    return "collectEnv";
  }

  @Override
  public String getDescription() {
    return "Collect environment information by running a set of commands. ";
  }
}