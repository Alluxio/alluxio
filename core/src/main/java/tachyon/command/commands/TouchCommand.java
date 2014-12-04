package tachyon.command.commands;

import java.io.IOException;
import java.io.OutputStream;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;

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
  public static final String NAME = "touch";
  public static final String DESCRIPTION =
            "Creates a 0 byte file specified by argv.";

  /**
   * Constructor with Closer
   *
   * @param c
   */
  public TouchCommand(Closer c) {
    super(c);
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException {
    return touch(cmdl);
  }

  /**
   * Creates a 0 byte file specified by argv.
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command if successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int touch(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    TachyonFile tFile = tachyonClient.getFile(tachyonClient.createFile(path));
    OutputStream out = tFile.getOutStream(WriteType.THROUGH);
    out.close();
    System.out.println(path + " has been created");
    return 0;
  }
}