package alluxio.cli.bundler.command;

import alluxio.cli.CommandUtils;
import alluxio.cli.ConfigurationDocGenerator;
import alluxio.cli.bundler.RunCommandUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectEnvCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectEnvCommand.class);

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

  public CollectEnvCommand(@Nullable InstancedConfiguration conf) {
    super(conf);
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
    mCommands.add(new UnixCommand("list Alluxio home", String.format("ls -al -R %s", mConf.get(PropertyKey.HOME)), null));
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

    // Output buffer stream
    StringWriter output = new StringWriter();

    for (UnixCommand cmd : mCommands) {
      String[] command = cmd.getCommand();
      output.write(String.format("Running cmd %s", Arrays.toString(command)));
      RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(command);
      String cmdResult = String.format("StatusCode:%s\nStdOut:\n%s\nStdErr:\n%s", cr.getStatusCode(),
              cr.getStdOut(), cr.getStdErr());
      output.write(cmdResult);
    }

    // output the buffer
    File outputFile = new File(Paths.get(this.getWorkingDirectory(), this.getCommandName()).toUri());
    LOG.info(String.format("Finished all commands. Writing to output file %s", outputFile));
    FileUtils.writeStringToFile(outputFile, output.toString());

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

  @Override
  public String getWorkingDirectory() {
    // TODO(jiacheng): create if not exist
    return Paths.get(super.getWorkingDirectory(), this.getCommandName()).toString();
  }
}