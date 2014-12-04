package tachyon.command.commands;

import java.io.IOException;

import com.google.common.io.Closer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 *  Free the file or Folder from tachyon in-memory specified by argv
 */
public class FreeCommand extends AbstractCommands {
  public static final String NAME = "free";
  public static final String DESCRIPTION = "Free the file or Folder "
      + "from tachyon in-memory specified by argv.";

  /**
   * Constructor with Closer
   *
   * @param c
   */
  public FreeCommand(Closer c) {
    super(c);
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return free(cmdl);
  }

  /**
   * Free the file or Folder from tachyon in-memory specified by argv
   *
   * @param cmdl [] Array of arguments given by the user's input from the terminal
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws IOException
   */
  public int free(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.freepath(path, true)) {
      System.out.println(path + " was successfully freed from memory.");
      return 0;
    } else {
      return -1;
    }
  }
}