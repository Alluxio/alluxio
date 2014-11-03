package tachyon.command.commands;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.CommonUtils;

/**
 * Displays information for all directories and files directly under the path specified in argv.
 *
 */
public class LsCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv)  throws IOException {
      return ls(argv);
  }

  /**
  * Displays information for all directories and files directly under the path specified in argv.
  *
  * @param argv [] Array of arguments given by the user's input from the terminal
  * @return 0 if command is successful, -1 if an error occurred.
  * @throws java.io.IOException
  */
  public int ls(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs ls <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    String format = "%-10s%-25s%-15s%-5s%n";
    for (ClientFileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, CommonUtils.getSizeFromBytes(file.getLength()),
      CommonUtils.convertMsToDate(file.getCreationTimeMs()), inMemory, file.getPath());
    }
    return 0;
    }
}
