package tachyon.command.commands;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Displays a list of hosts that have the file specified in argv stored.
 *
 */
public class LocationCommand extends AbstractCommands {
  public static final String NAME = "location";
  public static final String DESCRIPTION =
          "Displays a list of hosts that have the file specified in argv stored.";

    @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return location(cmdl);
  }

  /**
   * Displays a list of hosts that have the file specified in argv stored.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int location(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    List<String> hosts = tachyonClient.getFile(fileId).getLocationHosts();
    System.out.println(path + " with file id " + fileId + " is on nodes: ");
    for (String host : hosts) {
      System.out.println(host);
    }
    return 0;
  }
}