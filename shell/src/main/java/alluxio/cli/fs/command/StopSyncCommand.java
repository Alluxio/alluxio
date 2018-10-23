package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

public class StopSyncCommand extends AbstractFileSystemCommand {
  /**
   * @param fs the filesystem of Alluxio
   */
  public StopSyncCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "stopSync";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    mFileSystem.startSync(path);
    System.out.println("Stopped automatic syncing of '" + path + "'.");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "stopSync <path>";
  }

  @Override
  public String getDescription() {
    return "Stops the automatic syncing process of the specified path). ";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
