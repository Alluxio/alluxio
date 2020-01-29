package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

// TODO(jiacheng): Do we want to move this logic to InfoCollector shell and have finer granularity?
public class CollectAllCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectAllCommand.class);

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

  public CollectAllCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION)
            .addOption(COMPONENT_OPTION);
  }

  //
  List<AbstractInfoCollectorCommand> mChildren;

  @Override
  public String getCommandName() {
    return "collectAll";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    String targetDir = getDestDir(cl);

    // Invoke all other commands to collect information
    // FORCE_OPTION will be propagated to child commands
    for (AbstractInfoCollectorCommand child : mChildren) {
      LOG.info(String.format("Executing command %s", child.getCommandName()));
      ret = child.run(cl);
      LOG.info(String.format("Command return %s", ret));
    }

    // Generate bundle
    LOG.info(String.format("Archiving dir %s", targetDir));
    String tarballPath = Paths.get(targetDir, "collectAll.tar.gz").toAbsolutePath().toString();
    TarUtils.compress(tarballPath, Files.list(new File(targetDir).toPath()).toArray(File[]::new));
    LOG.info("Archiving finished");

    return ret;
  }


  @Override
  public String getUsage() {
    return "collectAll";
  }

  @Override
  // TODO(jiacheng): desc
  public String getDescription() {
    return "Collect all information.";
  }
}
