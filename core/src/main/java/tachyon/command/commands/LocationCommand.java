package tachyon.command.commands;

import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Displays a list of hosts that have the file specified in argv stored.
 *
 */
public class LocationCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return location(argv);
  }

  /**
   * Displays a list of hosts that have the file specified in argv stored.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int location(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs location <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
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