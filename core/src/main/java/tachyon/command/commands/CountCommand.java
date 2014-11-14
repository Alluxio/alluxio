package tachyon.command.commands;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

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
  public static final String NAME = "count";
  public static final String DESCRIPTION =
          "Displays the number of folders and files matching the specified prefix in argv.";

  /**
   * Displays the number of folders and files matching the specified prefix in argv.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int count(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
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
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return count(cmdl);
  }
}