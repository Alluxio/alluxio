package tachyon.command.commands;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.command.AbstractCommands;

/**
 * 
 */
public class RequestCommand extends AbstractCommands {
  @Override
  public int execute(String[] argv) throws IOException {
    return request(argv);
  }

  /**
   *
   * @param argv
   * @return
   * @throws IOException
   */
  public int request(String[] argv) throws IOException {
    if (argv.length != 3) {
      System.out.println("Usage: tfs request <tachyonaddress> <dependencyId>");
      return -1;
    }
    TachyonURI path = new TachyonURI(argv[1]);
    int depId = Integer.parseInt(argv[2]);
    TachyonFS tachyonClient = createFS(path);
    tachyonClient.requestFilesInDependency(depId);
    System.out.println("Dependency with ID " + depId + " has been requested.");
    return 0;
  }
}