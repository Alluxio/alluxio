package tachyon.command.commands;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Renames a file or directory specified by argv. Will fail if the new path name already exists.
 *
 */
public class RenameCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return rename(argv);
  }

  /**
   * Renames a file or directory specified by argv. Will fail if the new path name already exists.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int rename(String[] argv) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs mv <src> <dst>");
      return -1;
    }
    TachyonURI srcPath = new TachyonURI(argv[1]);
    TachyonURI dstPath = new TachyonURI(argv[2]);
    TachyonFS tachyonClient = createFS(srcPath);
    if (tachyonClient.rename(srcPath, dstPath)) {
      System.out.println("Renamed " + srcPath + " to " + dstPath);
      return 0;
    } else {
      return -1;
    }
  }
}