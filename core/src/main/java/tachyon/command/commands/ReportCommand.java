package tachyon.command.commands;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * 
 */
public class ReportCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return report(argv);
  }

  /**
   *
   * @param argv
   * @return
   * @throws IOException
   */
  public int report(String[] argv) throws IOException {
    if (argv.length != 2) {
      System.out.println("Usage: tfs report <path>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    TachyonFS tachyonClient = createFS(path);
    int fileId = tachyonClient.getFileId(path);
    tachyonClient.reportLostFile(fileId);
    System.out.println(path + " with file id " + fileId + " has reported been report lost.");
    return 0;
  }
}
