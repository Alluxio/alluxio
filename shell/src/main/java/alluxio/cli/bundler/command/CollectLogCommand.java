package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
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

public class CollectLogCommand  extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectLogCommand.class);

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("ignores existing work")
                  .build();
  private static final Option COMPONENT_OPTION =
          Option.builder("c").longOpt("components")
                  .hasArg(true).desc("components to collect logs from")
                  .build();

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION)
            .addOption(COMPONENT_OPTION);
  }

  public CollectLogCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "collectLog";
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
    String workingDir = this.getWorkingDirectory(targetDir);
    String logDir = mFsContext.getClusterConf().get(PropertyKey.LOGS_DIR);

    // TODO(jiacheng): Copy intelligently
    // TODO(jiacheng): components option
    FileUtils.copyDirectory(new File(logDir), new File(workingDir), true);

    return ret;
  }

  @Override
  public String getUsage() {
    return "collectLog";
  }

  @Override
  public String getDescription() {
    return "Collect logs";
  }
}
