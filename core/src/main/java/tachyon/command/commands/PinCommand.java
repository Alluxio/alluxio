package tachyon.command.commands;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
 * never evicted from memory.
 *
 */
public class PinCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return pin(argv);
  }

  /**
   * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
   * never evicted from memory.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int pin(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs pin <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    tachyonClient.pinFile(fileId);
    try {
      tachyonClient.pinFile(fileId);
      System.out.println("File '" + path + "' was successfully pinned.");
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("File '" + path + "' could not be pinned.");
      return -1;
    }
  }
}