package tachyon.command.commands;

import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;
import tachyon.thrift.ClientBlockInfo;

/**
 * Displays the file's all blocks info
 *
 */
public class FileinfoCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return fileinfo(argv);
  }

  /**
   * Displays the file's all blocks info
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred.
   * @throws java.io.IOException
   */
  public int fileinfo(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs fileinfo <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
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
