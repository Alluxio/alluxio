package alluxio.cli.bundler.command;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class CollectLogCommand  extends AbstractInfoCollectorCommand {
  public CollectLogCommand(@Nullable InstancedConfiguration conf) {
    super(conf);
  }

  @Override
  public String getCommandName() {
    return "collectLog";
  }

  // TODO(jiacheng): Add arguments
  @Override
  public boolean hasSubCommand() {
    return false;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String workingDir = this.getWorkingDirectory();
    String logDir = mConf.get(PropertyKey.LOGS_DIR);

    // TODO(jiacheng): Copy intelligently
    FileUtils.copyDirectory(new File(logDir), new File(workingDir), true);

    return 0;
  }

  @Override
  public String getUsage() {
    return "collectLog";
  }

  @Override
  public String getDescription() {
    return "Collect logs";
  }

  @Override
  public String getWorkingDirectory() {
    return Paths.get(super.getWorkingDirectory(), this.getCommandName()).toString();
  }
}
