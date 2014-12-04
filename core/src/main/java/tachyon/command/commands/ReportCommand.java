package tachyon.command.commands;

import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 *
 */
public class ReportCommand extends AbstractCommands {
  public static final String NAME = "report";
  public static final String DESCRIPTION = "Report file.";

  /**
   * Constructor with Closer
   *
   * @param c
   */
  public ReportCommand(Closer c) {
    super(c);
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return report(cmdl);
  }

  /**
   * Report file.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int report(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    tachyonClient.reportLostFile(fileId);
    System.out.println(path + " with file id " + fileId + " has reported been report lost.");
    return 0;
  }
}
