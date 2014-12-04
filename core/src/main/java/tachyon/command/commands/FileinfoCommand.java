package tachyon.command.commands;

import java.io.IOException;
import java.util.List;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;
import tachyon.thrift.ClientBlockInfo;

/**
 * Displays the file's all blocks info
 *
 */
public class FileinfoCommand extends AbstractCommands {
  public static final String NAME = "fileinfo";
  public static final String DESCRIPTION = "Displays the file's all blocks info.";

  /**
   * Constructor with Closer
   *
   * @param c
   */
  public FileinfoCommand(Closer c) {
    super(c);
  }

  @Override
  public int execute(CommandLine cmdl) throws IOException, ParseException {
    return fileinfo(cmdl);
  }

  /**
   * Displays the file's all blocks info
   *
   * @param cmdl Arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int fileinfo(CommandLine cmdl) throws IOException {
    TachyonURI path = new TachyonURI(cmdl.getOptions()[0].getValue());
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    List<ClientBlockInfo> blocks = tachyonClient.getFileBlocks(fileId);
    System.out.println(path + " with file id " + fileId + " has the following blocks: ");
    for (ClientBlockInfo block : blocks) {
      System.out.println(block);
    }
    return 0;
  }
}
