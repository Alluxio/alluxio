package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.cli.bundler.InfoCollector;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.util.ConfigurationUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class CollectConfigCommand extends AbstractInfoCollectorCommand {
  public CollectConfigCommand(@Nullable InstancedConfiguration conf) {
    super(conf);
  }

  @Override
  public String getCommandName() {
    return "collectConfig";
  }

  @Override
  public boolean hasSubCommand() {
    return false;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String workingDir = this.getWorkingDirectory();
    String confDir = mConf.get(PropertyKey.CONF_DIR);

    // TODO(jiacheng): Copy intelligently
    FileUtils.copyDirectory(new File(confDir), new File(workingDir), true);

    return 0;
  }

  @Override
  public String getUsage() {
    return "collectConfig";
  }

  @Override
  public String getDescription() {
    return "Collect configurations";
  }

  @Override
  public String getWorkingDirectory() {
    return Paths.get(super.getWorkingDirectory(), this.getCommandName()).toString();
  }
}
