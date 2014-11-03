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
 * Displays information for all directories and files under the path specified in argv
 * recursively.
 *
 */
public class LsrCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return lsr(argv);
  }

  /**
   * Displays information for all directories and files under the path specified in argv
   * recursively.
   *
   * @param argv
   * @return
   * @throws IOException
   */
  public int lsr(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs lsr <path>");
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
      if (file.isFolder) {
        lsr(new String[] {"lsr", file.getPath()});
      }
    }
    return 0;
  }
}
