package tachyon.command.commands;

import java.io.IOException;
import java.io.OutputStream;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.command.AbstractCommands;

/**
 * Creates a 0 byte file specified by argv.
 *
 */
public class TouchCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return touch(argv);
  }

  /**
   * Creates a 0 byte file specified by argv.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int touch(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs touch <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(tachyonClient.createFile(path));
    OutputStream out = tFile.getOutStream(WriteType.THROUGH);
    out.close();
    System.out.println(path + " has been created");
    return 0;
  }
}