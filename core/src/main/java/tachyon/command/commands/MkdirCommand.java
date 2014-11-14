package tachyon.command.commands;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Creates a new directory specified by the path in argv, including any parent folders that are
 * required. This method fails if a directory or file with the same path already exists.
 *
 */
public class MkdirCommand extends AbstractCommands {
  public static final String NAME = "mkdir";
  public static final String DESCRIPTION =
            "Creates a new directory specified by the path in argv.";

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return mkdir(cmdl);
  }

  /**
   * Creates a new directory specified by the path in argv, including any parent folders that are
   * required. This method fails if a directory or file with the same path already exists.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int mkdir(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.mkdir(path)) {
      System.out.println("Successfully created directory " + path);
      return 0;
    } else {
      return -1;
    }
  }
}
