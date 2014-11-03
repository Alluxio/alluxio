package tachyon.command.commands;

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.command.AbstractCommands;

/**
 * Prints the file's last 1KB of contents to the console.
 *
 */
public class TailCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return tail(argv);
  }
  
  /**
   * Prints the file's last 1KB of contents to the console.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.f
   * @throws java.io.IOException
   */
  public int tail(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs tail <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile == null) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      try {
        byte[] buf = new byte[Constants.KB];
        long bytesToRead = 0L;
        if (tFile.length() > Constants.KB) {
          bytesToRead = Constants.KB;
        } else {
          bytesToRead = tFile.length();
        }
        is.skip(tFile.length() - bytesToRead);
        int read = is.read(buf);
        System.out.write(buf, 0, read);
        return 0;
      } finally {
        is.close();
      }
    } else {
      System.out.println(path + " is not a file.");
      return -1;
    }
  }
}