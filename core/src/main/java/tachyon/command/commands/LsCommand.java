package tachyon.command.commands;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

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
  public static final String NAME = "ls";
  public static final String DESCRIPTION =
  "Displays information for all directories and files directly under the path specified in argv.";

  @Override
  public int execute(CommandLine cmdl)  throws IOException, ParseException {
      return ls(cmdl);
  }

  /**
  * Displays information for all directories and files directly under the path specified in argv.
  *
  * @param cmdl Arguments given by the user's input from the terminal
  * @return 0 if command is successful, -1 if an error occurred.
  * @throws java.io.IOException
  */
  public int ls(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
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
