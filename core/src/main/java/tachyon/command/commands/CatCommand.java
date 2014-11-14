package tachyon.command.commands;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.command.AbstractCommands;

/**
 * Prints the file's contents to the console.
 *
 */
public class CatCommand extends AbstractCommands {
  public static final String NAME = "cat";
  public static final String DESCRIPTION = "Prints the file's contents to the console.";

  /**
   * Prints the file's contents to the console.
   *
   * @param cmdl  parse of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int cat(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(path);

    if (tFile == null) {
      System.out.println(path + " does not exist.");
      return -1;
    }
    if (tFile.isFile()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      byte[] buf = new byte[512];
      try {
        int read = is.read(buf);
        while (read != -1) {
          System.out.write(buf, 0, read);
          read = is.read(buf);
        }
      } finally {
        is.close();
      }
      return 0;
    } else {
      System.out.println(path + " is not a file.");
      return -1;
    }
  }

  @Override
  public int execute(CommandLine cmdl)  throws IOException, ParseException {
    return cat(cmdl);
  }
}