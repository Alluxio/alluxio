package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

public class GetSyncPathListCommand extends AbstractFileSystemCommand{
  /**
   * @param fs the filesystem of Alluxio
   */
  public GetSyncPathListCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "getSyncPathList";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    List<String> files = mFileSystem.getSyncPathList();
    System.out.println("The following paths are under active sync");
    for (String file : files) {
      System.out.println(file);
    }
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
    return "getSyncPathList";
  }

  @Override
  public String getDescription() {
    return "Gets all the paths that are under active syncing right now.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 0);
  }
}
