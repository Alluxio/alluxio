package tachyon.command.commands;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Creates a new directory specified by the path in argv, including any parent folders that are
 * required. This method fails if a directory or file with the same path already exists.
 *
 */
public class MkdirCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return mkdir(argv);
  }

  /**
   * Creates a new directory specified by the path in argv, including any parent folders that are
   * required. This method fails if a directory or file with the same path already exists.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int mkdir(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs mkdir <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.mkdir(path)) {
      System.out.println("Successfully created directory " + path);
      return 0;
    } else {
      return -1;
    }
  }
}
