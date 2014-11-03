package tachyon.command.commands;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.command.AbstractCommands;
import tachyon.thrift.ClientFileInfo;

/**
 * Displays the number of folders and files matching the specified prefix in argv.
 *
 */
public class CountCommand extends AbstractCommands {
  /**
   * Displays the number of folders and files matching the specified prefix in argv.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int count(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs count <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    long[] values = countHelper(path);
    String format = "%-25s%-25s%-15s%n";
    System.out.format(format, "File Count", "Folder Count", "Total Bytes");
    System.out.format(format, values[0], values[1], values[2]);
    return 0;
  }

  private long[] countHelper(TachyonURI path) throws IOException {
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile.isFile()) {
      return new long[] {1L, 0L, tFile.length()};
    }

    long[] rtn = new long[] {0L, 1L, 0L};

    List<ClientFileInfo> files = tachyonClient.listStatus(path);
    Collections.sort(files);
    for (ClientFileInfo file : files) {
      long[] toAdd = countHelper(new TachyonURI(file.getPath()));
      rtn[0] += toAdd[0];
      rtn[1] += toAdd[1];
      rtn[2] += toAdd[2];
    }
    return rtn;
  }

  @Override
  public int execute(String[] argv) throws IOException {
    return count(argv);
  }
}