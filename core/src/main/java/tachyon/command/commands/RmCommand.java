package tachyon.command.commands;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Removes the file or directory specified by argv. Will remove all files and directories in the
 * directory if a directory is specified.
 *
 */
public class RmCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return rm(argv);
  }
 
  /**
   * Removes the file or directory specified by argv. Will remove all files and directories in the
   * directory if a directory is specified.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int rm(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs rm <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.delete(path, true)) {
      System.out.println(path + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }
}
