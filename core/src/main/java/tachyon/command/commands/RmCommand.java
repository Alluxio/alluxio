package tachyon.command.commands;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Removes the file or directory specified by argv. Will remove all files and directories in the
 * directory if a directory is specified.
 *
 */
public class RmCommand extends AbstractCommands {
  public static final String NAME = "rm";
  public static final String DESCRIPTION =
            "Removes the file or directory specified by argv.";

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return rm(cmdl);
  }
 
  /**
   * Removes the file or directory specified by argv. Will remove all files and directories in the
   * directory if a directory is specified.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int rm(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    if (tachyonClient.delete(path, true)) {
      System.out.println(path + " has been removed");
      return 0;
    } else {
      return -1;
    }
  }
}
