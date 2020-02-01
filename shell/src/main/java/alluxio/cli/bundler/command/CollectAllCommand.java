package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.cli.bundler.InfoCollector;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  // TODO(jiacheng): target dir where is it defined?

  public CollectAllCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION)
            .addOption(COMPONENT_OPTION);
  }

  // TODO(jiacheng):
  Map<String, Command> mChildren;

  @Override
  public String getCommandName() {
    return "collectAll";
  }

  @Override
  // TODO(jiacheng): Do not throw these
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Load the commands
    if (mChildren == null) {
      mChildren = InfoCollector.getIndividualCommands();
    }

    // Determine the working dir path
    String targetDirPath = getDestDir(cl);

    // Invoke all other commands to collect information
    // FORCE_OPTION will be propagated to child commands
    List<Exception> exceptions = new ArrayList<>();
    for (String cmdName : mChildren.keySet()) {
      AbstractInfoCollectorCommand child = (AbstractInfoCollectorCommand) mChildren.get(cmdName);
      String preExecMsg = String.format("Executing command %s", child.getCommandName());
      System.out.println(preExecMsg);
      LOG.info(preExecMsg);
      try {
        ret = child.run(cl);
        if (ret != 0) {
          String errorExitMsg = String.format("Command %s completed with a non-zero exit code %s",
                  child.getCommandName(), ret);
          System.out.println(errorExitMsg);
          LOG.warn(errorExitMsg);
        }
      } catch (IOException | AlluxioException e) {
        String exMsg = String.format("Exception when running %s: %s",
                child.getCommandName(), e.getMessage());
        exceptions.add(e);
      }
    }

    // Generate bundle
    System.out.println(String.format("Archiving dir %s", targetDirPath));
    LOG.info(String.format("Archiving dir %s", targetDirPath));
    String tarballPath = Paths.get(targetDirPath, "collectAll.tar.gz").toAbsolutePath().toString();
    // All files to compress
    File targetDir = new File(targetDirPath);
    // TODO(jiacheng): verify

    File[] files = targetDir.listFiles();
    TarUtils.compress(tarballPath, files);
    System.out.println("Archiving finished");
    LOG.info("Archiving finished");

    // TODO(jiacheng): what to do with the exceptions?

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
