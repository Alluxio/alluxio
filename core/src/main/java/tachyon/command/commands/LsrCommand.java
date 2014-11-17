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
 * Displays information for all directories and files under the path specified in argv
 * recursively.
 *
 */
public class LsrCommand extends AbstractCommands {
  public static final String NAME = "lsr";
  public static final String DESCRIPTION =
      "Displays information for all directories and files under the path specified recursively.";

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return lsr(cmdl.getOptions()[0].getValue());
  }

  /**
   * Displays information for all directories and files under the path specified in argv
   * recursively.
   *
   * @param fileParse Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws IOException
   */
  public int lsr(String fileParse) throws IOException {
    TachyonURI path = new TachyonURI(fileParse);
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
      if (file.isFolder) {
        lsr(file.getPath());
      }
    }
    return 0;
  }
}
