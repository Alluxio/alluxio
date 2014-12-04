package tachyon.command.commands;

import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * Renames a file or directory specified by argv. Will fail if the new path name already exists.
 *
 */
public class RenameCommand extends AbstractCommands {
  public static final String NAME = "rename";
  public static final String DESCRIPTION =
          "Renames a file or directory specified by argv.";

  /**
   * Constructor with Closer
   *
   * @param c
   */
  public RenameCommand(Closer c) {
    super(c);
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return rename(cmdl);
  }

  /**
   * Renames a file or directory specified by argv. Will fail if the new path name already exists.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException, ParseException
   */
  public int rename(CommandLine cmdl) throws IOException, ParseException {
    TachyonURI srcPath = null;
    TachyonURI dstPath = null;
    try {
      srcPath = new TachyonURI(cmdl.getOptions()[0].getValue(0));
      dstPath = new TachyonURI(cmdl.getOptions()[0].getValue(1));
    } catch (Exception e) {
      throw new ParseException("Missing second argument for option: rename");
    }
    TachyonFS tachyonClient = createFS(srcPath);
    if (tachyonClient.rename(srcPath, dstPath)) {
      System.out.println("Renamed " + srcPath + " to " + dstPath);
      return 0;
    } else {
      return -1;
    }
  }
}