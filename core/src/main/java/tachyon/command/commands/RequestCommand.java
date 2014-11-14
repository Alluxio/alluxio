package tachyon.command.commands;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * 
 */
public class RequestCommand extends AbstractCommands {
  public static final String NAME = "request";
  public static final String DESCRIPTION =
            "falta docu";

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return request(cmdl);
  }

  /**
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int request(CommandLine cmdl) throws IOException, ParseException {
    TachyonURI path = null;
    int depId;
    try {
      path = new TachyonURI(cmdl.getOptions()[0].getValue(0));
      depId = Integer.parseInt(cmdl.getOptions()[0].getValue(1));
    } catch (Exception e) {
      throw new ParseException("Missing second argument for option: copyToLocal");
    }
    TachyonFS tachyonClient = createFS(path);
    tachyonClient.requestFilesInDependency(depId);
    System.out.println("Dependency with ID " + depId + " has been requested.");
    return 0;
  }
}